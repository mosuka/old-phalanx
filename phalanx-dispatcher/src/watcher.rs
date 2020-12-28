use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{anyhow, Error, Result};
use crossbeam::channel::{unbounded, TryRecvError};
use hash_ring::HashRing;
use highway::HighwayBuildHasher;
use lazy_static::lazy_static;
use log::*;
use regex::Regex;
use tokio::sync::RwLock;
use tonic::codegen::Arc;
use tonic::transport::Channel;

use phalanx_kvs::kvs::nop::TYPE as NOP_TYPE;
use phalanx_kvs::kvs::{EventType, KVSContainer};
use phalanx_proto::phalanx::index_service_client::IndexServiceClient;
use phalanx_proto::phalanx::NodeDetails;

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

pub struct Watcher {
    kvs_container: KVSContainer,
    watching: Arc<AtomicBool>,
    unwatch: Arc<AtomicBool>,
    pub metadata_map: Arc<RwLock<HashMap<String, HashMap<String, HashMap<String, NodeDetails>>>>>,
    pub client_map:
        Arc<RwLock<HashMap<String, HashMap<String, HashMap<String, IndexServiceClient<Channel>>>>>>,
    pub shard_ring_map: Arc<RwLock<HashMap<String, HashRing<String, HighwayBuildHasher>>>>,
}

impl Watcher {
    pub async fn new(kvs_container: KVSContainer) -> Watcher {
        Watcher {
            kvs_container,
            watching: Arc::new(AtomicBool::new(false)),
            unwatch: Arc::new(AtomicBool::new(false)),
            metadata_map: Arc::new(RwLock::new(HashMap::new())),
            client_map: Arc::new(RwLock::new(HashMap::new())),
            shard_ring_map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn watch(&mut self) -> Result<()> {
        if self.kvs_container.kvs.get_type() == NOP_TYPE {
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

        match self.kvs_container.kvs.watch(sender, "/").await {
            Ok(_) => (),
            Err(e) => {
                return Err(anyhow!(e.to_string()));
            }
        }

        let watching = Arc::clone(&self.watching);
        let unwatch = Arc::clone(&self.unwatch);

        // initialize nodes cache
        match self.kvs_container.kvs.list("/").await {
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
        if self.kvs_container.kvs.get_type() == NOP_TYPE {
            debug!("the NOP discovery does not do anything");
            return Ok(());
        }

        if !self.watching.load(Ordering::Relaxed) {
            let msg = "watcher is not running";
            warn!("{}", msg);
            return Err(anyhow!(msg));
        }

        match self.kvs_container.kvs.unwatch().await {
            Ok(_) => (),
            Err(e) => {
                return Err(anyhow!(e.to_string()));
            }
        }

        self.unwatch.store(true, Ordering::Relaxed);

        Ok(())
    }
}
