use std::collections::HashMap;
use std::hash::BuildHasherDefault;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{anyhow, Context, Error, Result};
use crossbeam::channel::{unbounded, TryRecvError};
use futures::future::try_join_all;
use hash_ring::HashRing;
use highway::HighwayHasher;
use lazy_static::lazy_static;
use log::*;
use rand::Rng;
use regex::Regex;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tonic::transport::Channel;
use tonic::Code;

use phalanx_discovery::discovery::nop::TYPE as NOP_TYPE;
use phalanx_discovery::discovery::{DiscoveryContainer, EventType};
use phalanx_proto::phalanx::index_service_client::IndexServiceClient;
use phalanx_proto::phalanx::{CommitReq, DeleteReq, GetReq, NodeDetails, Role, RollbackReq, SetReq, State};
use serde_json::Value;

const KEY_PATTERN: &'static str = r"^/([^/]+)/([^/]+)/([^/]+)\.json";

lazy_static! {
    static ref KEY_REGEX: Regex = Regex::new(KEY_PATTERN).unwrap();
}

fn parse_key(key: &str) -> Result<(String, String, String)> {
    match KEY_REGEX.captures(key) {
        Some(cap) => {
            let value1 = match cap.get(1) {
                Some(m) => m.as_str(),
                None => {
                    return Err(anyhow!("value does not match"));
                }
            };

            let value2 = match cap.get(2) {
                Some(m) => m.as_str(),
                None => {
                    return Err(anyhow!("value does not match"));
                }
            };

            let value3 = match cap.get(3) {
                Some(m) => m.as_str(),
                None => {
                    return Err(anyhow!("value does not match"));
                }
            };

            Ok((value1.to_string(), value2.to_string(), value3.to_string()))
        }
        None => Err(anyhow!("{:?} does not match {:?}", key, KEY_PATTERN)),
    }
}

type HighwayBuildHasher = BuildHasherDefault<HighwayHasher>;

#[derive(Clone)]
pub struct Client {
    discovery_container: DiscoveryContainer,
    watching: Arc<AtomicBool>,
    unwatch: Arc<AtomicBool>,
    metadata_map: Arc<RwLock<HashMap<String, HashMap<String, HashMap<String, NodeDetails>>>>>,
    client_map:
        Arc<RwLock<HashMap<String, HashMap<String, HashMap<String, IndexServiceClient<Channel>>>>>>,
    shard_ring_map: Arc<RwLock<HashMap<String, HashRing<String, HighwayBuildHasher>>>>,
}

impl Client {
    pub async fn new(discovery_container: DiscoveryContainer) -> Client {
        Client {
            discovery_container,
            watching: Arc::new(AtomicBool::new(false)),
            unwatch: Arc::new(AtomicBool::new(false)),
            metadata_map: Arc::new(RwLock::new(HashMap::new())),
            client_map: Arc::new(RwLock::new(HashMap::new())),
            shard_ring_map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn watch(&mut self, key: &str) -> Result<()> {
        if self.discovery_container.discovery.get_type() == NOP_TYPE {
            debug!("the NOP discovery does not do anything");
            return Ok(());
        }

        if self.watching.load(Ordering::Relaxed) {
            let msg = "receiver is already running";
            warn!("{}", &msg);
            return Err(anyhow!(msg));
        }

        // initialize
        self.metadata_map = Arc::new(RwLock::new(HashMap::new()));
        self.client_map = Arc::new(RwLock::new(HashMap::new()));
        self.shard_ring_map = Arc::new(RwLock::new(HashMap::new()));
        let (sender, receiver) = unbounded();

        match self.discovery_container.discovery.watch(sender, key).await {
            Ok(_) => (),
            Err(e) => {
                return Err(anyhow!(e.to_string()));
            }
        }

        let watching = Arc::clone(&self.watching);
        let unwatch = Arc::clone(&self.unwatch);

        // initialize nodes cache
        let key = "/";
        match self.discovery_container.discovery.list(key).await {
            Ok(kvps) => {
                for kvp in kvps {
                    // check key format
                    let (index_name, shard_name, node_name) = match parse_key(&kvp.key) {
                        Ok((index_name, shard_name, node_name)) => {
                            (index_name, shard_name, node_name)
                        }
                        Err(e) => {
                            // ignore keys that do not match the pattern
                            error!("{}", e.to_string());
                            continue;
                        }
                    };

                    if node_name == "_index_meta" {
                        // skip index metadata
                        continue;
                    }

                    // make metadata
                    let node_details =
                        match serde_json::from_slice::<NodeDetails>(kvp.value.as_slice()) {
                            Ok(node_details) => node_details,
                            Err(err) => {
                                error!("failed to parse JSON: error={:?}", err);
                                continue;
                            }
                        };

                    // add client
                    let address = format!("http://{}", node_details.address.clone());
                    match IndexServiceClient::connect(address).await {
                        Ok(client) => {
                            let mut client_indices = self.client_map.write().await;
                            if !client_indices.contains_key(&index_name) {
                                let client_shards = HashMap::new();
                                client_indices.insert(index_name.clone(), client_shards);
                            }
                            if !client_indices
                                .get(&index_name)
                                .unwrap()
                                .contains_key(&shard_name)
                            {
                                let client_nodes = HashMap::new();
                                client_indices
                                    .get_mut(&index_name)
                                    .unwrap()
                                    .insert(shard_name.clone(), client_nodes);
                            }
                            client_indices
                                .get_mut(&index_name)
                                .unwrap()
                                .get_mut(&shard_name)
                                .unwrap()
                                .insert(node_name.clone(), client);
                        }
                        Err(e) => {
                            error!(
                                "failed to connect to {}: {}",
                                &node_details.address,
                                e.to_string()
                            );
                        }
                    };

                    // add node metadata
                    let mut metadata_indices = self.metadata_map.write().await;
                    if !metadata_indices.contains_key(&index_name) {
                        let metadata_shards = HashMap::new();
                        metadata_indices.insert(index_name.clone(), metadata_shards);
                    }
                    if !metadata_indices
                        .get(&index_name)
                        .unwrap()
                        .contains_key(&shard_name)
                    {
                        let metadata_nodes = HashMap::new();
                        metadata_indices
                            .get_mut(&index_name)
                            .unwrap()
                            .insert(shard_name.clone(), metadata_nodes);
                    }
                    metadata_indices
                        .get_mut(&index_name)
                        .unwrap()
                        .get_mut(&shard_name)
                        .unwrap()
                        .insert(node_name.clone(), node_details);

                    // add shard ring
                    let mut ring_indices = self.shard_ring_map.write().await;
                    if !ring_indices.contains_key(&index_name) {
                        let ring_nodes: Vec<String> = Vec::new();
                        let ring_shards: HashRing<String, HighwayBuildHasher> =
                            HashRing::with_hasher(ring_nodes, 10, HighwayBuildHasher::default());
                        ring_indices.insert(index_name.clone(), ring_shards);
                    }
                    ring_indices
                        .get_mut(&index_name)
                        .unwrap()
                        .add_node(&shard_name);
                }
            }
            Err(err) => {
                error!("failed to list: error={:?}", err);
            }
        };

        let metadata_map = Arc::clone(&self.metadata_map);
        let client_map = Arc::clone(&self.client_map);
        let shard_ring_map = Arc::clone(&self.shard_ring_map);

        tokio::spawn(async move {
            debug!("start cluster watch thread");
            watching.store(true, Ordering::Relaxed);

            loop {
                match receiver.try_recv() {
                    Ok(event) => {
                        // check key format
                        let (index_name, shard_name, node_name) = match parse_key(&event.key) {
                            Ok((index_name, shard_name, node_name)) => {
                                (index_name, shard_name, node_name)
                            }
                            Err(e) => {
                                // ignore keys that do not match the pattern
                                debug!("parse error: error={:?}", e);
                                continue;
                            }
                        };

                        if node_name == "_index_meta" {
                            // skip index metadata
                            continue;
                        }

                        match event.event_type {
                            EventType::Put => {
                                // make metadata
                                let node_details = match serde_json::from_slice::<NodeDetails>(
                                    event.value.as_slice(),
                                ) {
                                    Ok(node_details) => node_details,
                                    Err(err) => {
                                        error!("failed to parse JSON: error={:?}", err);
                                        continue;
                                    }
                                };

                                // add client
                                let address = format!("http://{}", node_details.address.clone());
                                match IndexServiceClient::connect(address).await {
                                    Ok(client) => {
                                        let mut client_indices = client_map.write().await;
                                        if !client_indices.contains_key(&index_name) {
                                            let client_shards = HashMap::new();
                                            client_indices
                                                .insert(index_name.clone(), client_shards);
                                        }
                                        if !client_indices
                                            .get(&index_name)
                                            .unwrap()
                                            .contains_key(&shard_name)
                                        {
                                            let client_nodes = HashMap::new();
                                            client_indices
                                                .get_mut(&index_name)
                                                .unwrap()
                                                .insert(shard_name.clone(), client_nodes);
                                        }
                                        client_indices
                                            .get_mut(&index_name)
                                            .unwrap()
                                            .get_mut(&shard_name)
                                            .unwrap()
                                            .insert(node_name.clone(), client);
                                    }
                                    Err(e) => {
                                        error!(
                                            "failed to connect to {}: {}",
                                            &node_details.address,
                                            e.to_string()
                                        );
                                    }
                                };

                                // add node metadata
                                let mut metadata_indices = metadata_map.write().await;
                                if !metadata_indices.contains_key(&index_name) {
                                    let metadata_shards = HashMap::new();
                                    metadata_indices.insert(index_name.clone(), metadata_shards);
                                }
                                if !metadata_indices
                                    .get(&index_name)
                                    .unwrap()
                                    .contains_key(&shard_name)
                                {
                                    let metadata_nodes = HashMap::new();
                                    metadata_indices
                                        .get_mut(&index_name)
                                        .unwrap()
                                        .insert(shard_name.clone(), metadata_nodes);
                                }
                                metadata_indices
                                    .get_mut(&index_name)
                                    .unwrap()
                                    .get_mut(&shard_name)
                                    .unwrap()
                                    .insert(node_name.clone(), node_details);

                                // add shard ring
                                let mut ring_indices = shard_ring_map.write().await;
                                if !ring_indices.contains_key(&index_name) {
                                    let ring_nodes: Vec<String> = Vec::new();
                                    let ring_shards: HashRing<String, HighwayBuildHasher> =
                                        HashRing::with_hasher(
                                            ring_nodes,
                                            10,
                                            HighwayBuildHasher::default(),
                                        );
                                    ring_indices.insert(index_name.clone(), ring_shards);
                                }
                                ring_indices
                                    .get_mut(&index_name)
                                    .unwrap()
                                    .add_node(&shard_name);
                            }
                            EventType::Delete => {
                                // delete client
                                let mut client_indices = client_map.write().await;
                                if client_indices.contains_key(&index_name) {
                                    if client_indices
                                        .get(&index_name)
                                        .unwrap()
                                        .contains_key(&shard_name)
                                    {
                                        if client_indices
                                            .get(&index_name)
                                            .unwrap()
                                            .get(&shard_name)
                                            .unwrap()
                                            .contains_key(&node_name)
                                        {
                                            client_indices
                                                .get_mut(&index_name)
                                                .unwrap()
                                                .get_mut(&shard_name)
                                                .unwrap()
                                                .remove(&node_name);
                                        }
                                        if client_indices
                                            .get(&index_name)
                                            .unwrap()
                                            .get(&shard_name)
                                            .unwrap()
                                            .len()
                                            <= 0
                                        {
                                            client_indices
                                                .get_mut(&index_name)
                                                .unwrap()
                                                .remove(&shard_name);
                                        }
                                    }
                                    if client_indices.get(&index_name).unwrap().len() <= 0 {
                                        client_indices.remove(&index_name);
                                    }
                                }

                                // delete metdata
                                let mut metadata_indices = metadata_map.write().await;
                                if metadata_indices.contains_key(&index_name) {
                                    if metadata_indices
                                        .get(&index_name)
                                        .unwrap()
                                        .contains_key(&shard_name)
                                    {
                                        if metadata_indices
                                            .get(&index_name)
                                            .unwrap()
                                            .get(&shard_name)
                                            .unwrap()
                                            .contains_key(&node_name)
                                        {
                                            metadata_indices
                                                .get_mut(&index_name)
                                                .unwrap()
                                                .get_mut(&shard_name)
                                                .unwrap()
                                                .remove(&node_name);
                                        }
                                        if metadata_indices
                                            .get(&index_name)
                                            .unwrap()
                                            .get(&shard_name)
                                            .unwrap()
                                            .len()
                                            <= 0
                                        {
                                            metadata_indices
                                                .get_mut(&index_name)
                                                .unwrap()
                                                .remove(&shard_name);
                                        }
                                    }
                                    if metadata_indices.get(&index_name).unwrap().len() <= 0 {
                                        metadata_indices.remove(&index_name);
                                    }
                                }

                                // delete shard ring
                                let mut ring_indices = shard_ring_map.write().await;
                                if ring_indices.contains_key(&index_name) {
                                    ring_indices
                                        .get_mut(&index_name)
                                        .unwrap()
                                        .remove_node(&shard_name);
                                }
                            }
                        };
                    }
                    Err(TryRecvError::Disconnected) => {
                        debug!("channel disconnected");
                        break;
                    }
                    Err(TryRecvError::Empty) => {
                        if unwatch.load(Ordering::Relaxed) {
                            debug!("receive a stop signal");
                            // restore unreceive to false
                            unwatch.store(false, Ordering::Relaxed);
                            break;
                        }
                    }
                }
            }

            watching.store(false, Ordering::Relaxed);
            debug!("stop cluster watch thread");
        });

        Ok(())
    }

    pub async fn unwatch(&mut self) -> Result<(), Error> {
        if self.discovery_container.discovery.get_type() == NOP_TYPE {
            debug!("the NOP discovery does not do anything");
            return Ok(());
        }

        if !self.watching.load(Ordering::Relaxed) {
            let msg = "watcher is not running";
            warn!("{}", msg);
            return Err(anyhow!(msg));
        }

        match self.discovery_container.discovery.unwatch().await {
            Ok(_) => (),
            Err(e) => {
                return Err(anyhow!(e.to_string()));
            }
        }

        self.unwatch.store(true, Ordering::Relaxed);

        Ok(())
    }

    pub async fn metadata(
        &mut self,
        index_name: Option<&str>,
        shard_name: Option<&str>,
        node_name: Option<&str>,
    ) -> Result<HashMap<String, HashMap<String, HashMap<String, NodeDetails>>>> {
        let indices = self.metadata_map.read().await;
        debug!("metadata: {:?}", &indices);

        let mut metadata = HashMap::new();
        match index_name {
            Some(index) => {
                if indices.contains_key(index) {
                    metadata.insert(index.to_string(), HashMap::new());
                    let shards = indices.get(index).unwrap();
                    match shard_name {
                        Some(shard) => {
                            if shards.contains_key(shard) {
                                metadata
                                    .get_mut(index)
                                    .unwrap()
                                    .insert(shard.to_string(), HashMap::new());
                                let nodes = shards.get(shard).unwrap();
                                match node_name {
                                    Some(node) => {
                                        if nodes.contains_key(node) {
                                            metadata
                                                .get_mut(index)
                                                .unwrap()
                                                .get_mut(shard)
                                                .unwrap()
                                                .insert(
                                                    node.to_string(),
                                                    nodes.get(node).unwrap().clone(),
                                                );
                                        }
                                    }
                                    None => {
                                        metadata
                                            .get_mut(index)
                                            .unwrap()
                                            .insert(shard.to_string(), HashMap::new());
                                        for (node, _metadata) in nodes.iter() {
                                            metadata
                                                .get_mut(index)
                                                .unwrap()
                                                .get_mut(shard)
                                                .unwrap()
                                                .insert(node.to_string(), _metadata.clone());
                                        }
                                    }
                                }
                            }
                        }
                        None => {
                            metadata.insert(index.to_string(), HashMap::new());
                            for (shard, nodes) in shards.iter() {
                                metadata
                                    .get_mut(index)
                                    .unwrap()
                                    .insert(shard.to_string(), HashMap::new());
                                for (node, _metadata) in nodes.iter() {
                                    metadata
                                        .get_mut(index)
                                        .unwrap()
                                        .get_mut(shard)
                                        .unwrap()
                                        .insert(node.to_string(), _metadata.clone());
                                }
                            }
                        }
                    }
                }
            }
            None => {
                for (index, shards) in indices.iter() {
                    metadata.insert(index.to_string(), HashMap::new());
                    for (shard, nodes) in shards.iter() {
                        metadata
                            .get_mut(index)
                            .unwrap()
                            .insert(shard.to_string(), HashMap::new());
                        for (node, _metadata) in nodes.iter() {
                            metadata
                                .get_mut(index)
                                .unwrap()
                                .get_mut(shard)
                                .unwrap()
                                .insert(node.to_string(), _metadata.clone());
                        }
                    }
                }
            }
        }

        Ok(metadata)
    }

    pub async fn get(&mut self, id: &str, index_name: &str) -> Result<Vec<u8>> {
        let metadata = self.metadata_map.read().await;
        debug!("metadata: {:?}", &metadata);

        let client_map = self.client_map.read().await;

        let mut handles: Vec<JoinHandle<Result<Vec<u8>, Error>>> = Vec::new();
        let metadata_shards = match metadata.get(index_name) {
            Some(shards) => shards,
            None => return Err(anyhow!("{} does not exist", index_name)),
        };

        for (shard_name, m_nodes) in metadata_shards {
            let mut primary = "";
            let mut replicas: Vec<&str> = Vec::new();
            for (node, node_details) in m_nodes {
                let state = State::from_i32(node_details.state).unwrap();
                let role = Role::from_i32(node_details.role).unwrap();
                if state == State::Ready {
                    match role {
                        Role::Primary => primary = node,
                        Role::Replica => replicas.push(node),
                        Role::Candidate => {
                            debug!("{} is {:?}", node, role)
                        }
                    }
                }
            }

            debug!("primary: {}", &primary);
            debug!("replica: {:?}", &replicas);

            let node_name;
            if replicas.len() > 0 {
                let idx = rand::thread_rng().gen_range(0, replicas.len());
                node_name = replicas.get(idx).unwrap().clone();
            } else {
                node_name = primary;
            }

            let client_indices = client_map.clone();
            let index_name = index_name.to_string();
            let shard_name = shard_name.to_string();
            let node_name = node_name.to_string();

            let id = id.to_string();

            let handle = tokio::spawn(async move {
                if node_name.len() <= 0 {
                    Err(anyhow!("there is no available primary node"))
                } else {
                    match client_indices.get(&index_name) {
                        Some(client_shards) => match client_shards.get(&shard_name) {
                            Some(client_nodes) => match client_nodes.get(&node_name) {
                                Some(client) => {
                                    let req = tonic::Request::new(GetReq { id });
                                    let mut client: IndexServiceClient<Channel> = client.clone();
                                    match client.get(req).await {
                                        Ok(resp) => {
                                            let doc = resp.into_inner().doc;
                                            Ok(doc)
                                        }
                                        Err(e) => match e.code() {
                                            Code::NotFound => Ok(Vec::new()),
                                            _ => Err(Error::new(e)),
                                        },
                                    }
                                }
                                None => Err(anyhow!("{} does not exist", node_name)),
                            },
                            None => Err(anyhow!("{} does not exist", shard_name)),
                        },
                        None => Err(anyhow!("{} does not exist", index_name)),
                    }
                }
            });
            handles.push(handle);
        }

        let results = try_join_all(handles)
            .await
            .with_context(|| "failed to join handles")?;
        let results_cnt = results.len();

        let mut ret = Vec::new();
        let mut errs = Vec::new();
        for result in results {
            match result {
                Ok(doc) => {
                    if doc.len() > 0 {
                        ret = doc;
                        break;
                    }
                }
                Err(e) => {
                    error!("{}", e.to_string());
                    errs.push(anyhow!("{}", e.to_string()));
                }
            }
        }

        if results_cnt != errs.len() {
            Ok(ret)
        } else {
            let e = errs.get(0).unwrap().clone();
            Err(anyhow!("{}", e.to_string()))
        }
    }

    pub async fn put(&mut self, doc: Vec<u8>, index_name: &str, id_field: &str) -> Result<()> {
        let value: Value = match serde_json::from_slice::<Value>(&doc.as_slice()) {
            Ok(value) => value,
            Err(e) => {
                return Err(Error::new(e));
            }
        };
        let id = match value.as_object().unwrap()[id_field].as_str() {
            Some(id) => id,
            None => {
                return Err(anyhow!("{} does not exist", id_field));
            }
        };
        debug!("id field value is {:?}", id);

        let ring_indices = self.shard_ring_map.read().await;
        let shard_ring = match ring_indices.get(index_name) {
            Some(shard_ring) => shard_ring,
            None => return Err(anyhow!("{} does not exist", index_name)),
        };
        let shard_name = shard_ring.get_node(id.to_string()).unwrap();
        debug!("destination shard: {:?}", shard_name);

        let metadata = self.metadata_map.read().await;
        debug!("metadata: {:?}", &metadata);

        let metadata_nodes = match metadata.get(index_name) {
            Some(shards) => match shards.get(shard_name) {
                Some(nodes) => nodes,
                None => return Err(anyhow!("{} does not exist", shard_name)),
            },
            None => return Err(anyhow!("{} does not exist", index_name)),
        };

        let mut node_name = "";
        for (node, node_details) in metadata_nodes.iter() {
            let state = State::from_i32(node_details.state).unwrap();
            let role = Role::from_i32(node_details.role).unwrap();
            if state == State::Ready && role == Role::Primary {
                node_name = node.as_str();
                break;
            }
        }
        debug!("primary node is {:?}", node_name);

        let client_indices = self.client_map.read().await;
        let client = match client_indices.get(index_name) {
            Some(client_shards) => match client_shards.get(shard_name) {
                Some(client_nodes) => match client_nodes.get(node_name) {
                    Some(client) => client.clone(),
                    None => return Err(anyhow!("{} does not exist", node_name)),
                },
                None => return Err(anyhow!("{} does not exist", shard_name)),
            },
            None => return Err(anyhow!("{} does not exist", index_name)),
        };

        let mut client: IndexServiceClient<Channel> = client.into();
        let req = tonic::Request::new(SetReq { doc });
        match client.set(req).await {
            Ok(_resp) => Ok(()),
            Err(e) => Err(Error::new(e)),
        }
    }

    pub async fn delete(&mut self, id: &str, index_name: &str) -> Result<()> {
        let metadata = self.metadata_map.read().await;
        debug!("metadata: {:?}", &metadata);

        let client_map = self.client_map.read().await;

        let mut handles: Vec<JoinHandle<Result<(), Error>>> = Vec::new();
        let metadata_shards = match metadata.get(index_name) {
            Some(shards) => shards,
            None => return Err(anyhow!("{} does not exist", index_name)),
        };

        for (shard_name, nodes) in metadata_shards {
            let mut node_name = "";
            for (node, node_details) in nodes {
                let state = State::from_i32(node_details.state).unwrap();
                let role = Role::from_i32(node_details.role).unwrap();
                if state == State::Ready && role == Role::Primary {
                    node_name = node.as_str();
                    break;
                }
            }
            debug!("primary node is {:?}", node_name);

            let client_indices = client_map.clone();
            let index_name = index_name.to_string();
            let shard_name = shard_name.to_string();
            let node_name = node_name.to_string();
            let id = id.to_string();
            let handle = tokio::spawn(async move {
                if node_name.len() <= 0 {
                    Err(anyhow!("there is no available primary node"))
                } else {
                    match client_indices.get(&index_name) {
                        Some(client_shards) => match client_shards.get(&shard_name) {
                            Some(client_nodes) => match client_nodes.get(&node_name) {
                                Some(client) => {
                                    let req = tonic::Request::new(DeleteReq { id });
                                    let mut client: IndexServiceClient<Channel> = client.clone();
                                    match client.delete(req).await {
                                        Ok(_resp) => Ok(()),
                                        Err(e) => Err(Error::new(e)),
                                    }
                                }
                                None => Err(anyhow!("{} does not exist", node_name)),
                            },
                            None => Err(anyhow!("{} does not exist", shard_name)),
                        },
                        None => Err(anyhow!("{} does not exist", index_name)),
                    }
                }
            });
            handles.push(handle);
        }

        let results = try_join_all(handles)
            .await
            .with_context(|| "failed to join handles")?;
        let results_cnt = results.len();

        let mut errs = Vec::new();
        for result in results {
            match result {
                Ok(()) => (),
                Err(e) => {
                    error!("{}", &e.to_string());
                    errs.push(e);
                }
            }
        }

        if results_cnt != errs.len() {
            Ok(())
        } else {
            let e = errs.get(0).unwrap().to_string();
            Err(anyhow!(e))
        }
    }

    pub async fn commit(&mut self, index_name: &str) -> Result<()> {
        let metadata = self.metadata_map.read().await;
        debug!("metadata: {:?}", &metadata);

        let client_map = self.client_map.read().await;

        let mut handles: Vec<JoinHandle<Result<(), Error>>> = Vec::new();
        let metadata_shards = match metadata.get(index_name) {
            Some(shards) => shards,
            None => return Err(anyhow!("{} does not exist", index_name)),
        };

        for (shard_name, nodes) in metadata_shards {
            let mut node_name = "";
            for (node, node_details) in nodes {
                let state = State::from_i32(node_details.state).unwrap();
                let role = Role::from_i32(node_details.role).unwrap();
                if state == State::Ready && role == Role::Primary {
                    node_name = node.as_str();
                    break;
                }
            }
            debug!("primary node is {:?}", node_name);

            let client_indices = client_map.clone();
            let index_name = index_name.to_string();
            let shard_name = shard_name.to_string();
            let node_name = node_name.to_string();
            let handle = tokio::spawn(async move {
                if node_name.len() <= 0 {
                    Err(anyhow!("there is no available primary node"))
                } else {
                    match client_indices.get(&index_name) {
                        Some(client_shards) => match client_shards.get(&shard_name) {
                            Some(client_nodes) => match client_nodes.get(&node_name) {
                                Some(client) => {
                                    let req = tonic::Request::new(CommitReq {});
                                    let mut client: IndexServiceClient<Channel> = client.clone();
                                    match client.commit(req).await {
                                        Ok(_resp) => Ok(()),
                                        Err(e) => Err(Error::new(e)),
                                    }
                                }
                                None => Err(anyhow!("{} does not exist", node_name)),
                            },
                            None => Err(anyhow!("{} does not exist", shard_name)),
                        },
                        None => Err(anyhow!("{} does not exist", index_name)),
                    }
                }
            });
            handles.push(handle);
        }

        let results = try_join_all(handles)
            .await
            .with_context(|| "failed to join handles")?;
        let results_cnt = results.len();

        let mut errs = Vec::new();
        for result in results {
            match result {
                Ok(()) => (),
                Err(e) => {
                    error!("{}", &e.to_string());
                    errs.push(e);
                }
            }
        }

        if results_cnt != errs.len() {
            Ok(())
        } else {
            let e = errs.get(0).unwrap().to_string();
            Err(anyhow!(e))
        }
    }

    pub async fn rollback(&mut self, index_name: &str) -> Result<()> {
        let metadata = self.metadata_map.read().await;
        debug!("metadata: {:?}", &metadata);

        let client_map = self.client_map.read().await;

        let mut handles: Vec<JoinHandle<Result<(), Error>>> = Vec::new();
        let metadata_shards = match metadata.get(index_name) {
            Some(shards) => shards,
            None => return Err(anyhow!("{} does not exist", index_name)),
        };

        for (shard_name, nodes) in metadata_shards {
            let mut node_name = "";
            for (node, node_details) in nodes {
                let state = State::from_i32(node_details.state).unwrap();
                let role = Role::from_i32(node_details.role).unwrap();
                if state == State::Ready && role == Role::Primary {
                    node_name = node.as_str();
                    break;
                }
            }
            debug!("primary node is {:?}", node_name);

            let client_indices = client_map.clone();
            let index_name = index_name.to_string();
            let shard_name = shard_name.to_string();
            let node_name = node_name.to_string();
            let handle = tokio::spawn(async move {
                if node_name.len() <= 0 {
                    Err(anyhow!("there is no available primary node"))
                } else {
                    match client_indices.get(&index_name) {
                        Some(client_shards) => match client_shards.get(&shard_name) {
                            Some(client_nodes) => match client_nodes.get(&node_name) {
                                Some(client) => {
                                    let req = tonic::Request::new(RollbackReq {});
                                    let mut client: IndexServiceClient<Channel> = client.clone();
                                    match client.rollback(req).await {
                                        Ok(_resp) => Ok(()),
                                        Err(e) => Err(Error::new(e)),
                                    }
                                }
                                None => Err(anyhow!("{} does not exist", node_name)),
                            },
                            None => Err(anyhow!("{} does not exist", shard_name)),
                        },
                        None => Err(anyhow!("{} does not exist", index_name)),
                    }
                }
            });
            handles.push(handle);
        }

        let results = try_join_all(handles)
            .await
            .with_context(|| "failed to join handles")?;
        let results_cnt = results.len();

        let mut errs = Vec::new();
        for result in results {
            match result {
                Ok(()) => (),
                Err(e) => {
                    error!("{}", &e.to_string());
                    errs.push(e);
                }
            }
        }

        if results_cnt != errs.len() {
            Ok(())
        } else {
            let e = errs.get(0).unwrap().to_string();
            Err(anyhow!(e))
        }
    }
}
