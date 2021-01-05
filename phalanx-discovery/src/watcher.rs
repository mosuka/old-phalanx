use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

use anyhow::{anyhow, Error, Result};
use crossbeam::channel::{unbounded, TryRecvError};
use futures::future::try_join_all;
use lazy_static::lazy_static;
use log::*;
use regex::Regex;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use phalanx_kvs::kvs::etcd::{Etcd, EtcdConfig, TYPE as ETCD_TYPE};
use phalanx_kvs::kvs::nop::{Nop, TYPE as NOP_TYPE};
use phalanx_kvs::kvs::{EventType, KVSContainer};
use phalanx_proto::phalanx::index_service_client::IndexServiceClient;
use phalanx_proto::phalanx::{NodeDetails, ReadinessReq, Role, State};

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
    probe_interval: u64,
    receiving: Arc<AtomicBool>,
    unreceive: Arc<AtomicBool>,
    probing: Arc<AtomicBool>,
    unprobe: Arc<AtomicBool>,
    nodes: Arc<RwLock<HashMap<String, HashMap<String, HashMap<String, NodeDetails>>>>>,
}

impl Watcher {
    pub async fn new(kvs_container: KVSContainer, probe_interval: u64) -> Watcher {
        Watcher {
            kvs_container,
            probe_interval,
            receiving: Arc::new(AtomicBool::new(false)),
            unreceive: Arc::new(AtomicBool::new(false)),
            probing: Arc::new(AtomicBool::new(false)),
            unprobe: Arc::new(AtomicBool::new(false)),
            nodes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn watch(&mut self) -> Result<()> {
        if self.kvs_container.kvs.get_type() == NOP_TYPE {
            debug!("the NOP discovery does not do anything");
            return Ok(());
        }

        if self.receiving.load(Ordering::Relaxed) {
            let msg = "receiver is already running";
            warn!("{}", &msg);
            return Err(anyhow!(msg));
        }

        // initialize
        self.nodes = Arc::new(RwLock::new(HashMap::new()));
        let (sender, receiver) = unbounded();

        match self.kvs_container.kvs.watch(sender, "/").await {
            Ok(_) => (),
            Err(e) => {
                return Err(anyhow!(e.to_string()));
            }
        }

        let receiving = Arc::clone(&self.receiving);
        let unreceive = Arc::clone(&self.unreceive);

        let nodes = Arc::clone(&self.nodes);

        let kvs_container = match self.kvs_container.kvs.get_type() {
            ETCD_TYPE => {
                let config_json = self
                    .kvs_container
                    .kvs
                    .export_config_json()
                    .unwrap_or("".to_string());
                let config = match serde_json::from_str::<EtcdConfig>(config_json.as_str()) {
                    Ok(config) => config,
                    Err(e) => {
                        error!("failed to clone KVS container: error={:?}", e);
                        return Err(Error::from(e));
                    }
                };
                KVSContainer {
                    kvs: Box::new(Etcd::new(config)),
                }
            }
            _ => KVSContainer {
                kvs: Box::new(Nop::new()),
            },
        };
        // let discovery_container = self.kvs_container.clone();

        // update node metadata
        tokio::spawn(async move {
            debug!("start cluster watch thread");
            receiving.store(true, Ordering::Relaxed);

            {
                // initialize nodes cache
                let mut kvs_container = kvs_container.clone();
                match kvs_container.kvs.list("/").await {
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

                            // add node metadata
                            let mut metadata_indices = nodes.write().await;
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
                        }
                    }
                    Err(err) => {
                        error!("failed to list: error={:?}", err);
                    }
                }
            }

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

                                // add node metadata
                                let mut metadata_indices = nodes.write().await;
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
                            }
                            EventType::Delete => {
                                // delete metdata
                                let mut metadata_indices = nodes.write().await;
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
                            }
                        }

                        let mut kvs_container = kvs_container.clone();
                        let metadata_indices = nodes.read().await;

                        // candidate to replica
                        if metadata_indices.contains_key(&index_name) {
                            if metadata_indices
                                .get(&index_name)
                                .unwrap()
                                .contains_key(&shard_name)
                            {
                                let shards = metadata_indices
                                    .get(&index_name)
                                    .unwrap()
                                    .get(&shard_name)
                                    .unwrap();
                                for (tmp_node_name, node_details) in shards.iter() {
                                    if node_details.state == State::Ready as i32
                                        && node_details.role == Role::Candidate as i32
                                    {
                                        let mut new_node_details = node_details.clone();
                                        new_node_details.role = Role::Replica as i32;

                                        let key = format!(
                                            "/{}/{}/{}.json",
                                            &index_name, &shard_name, tmp_node_name
                                        );
                                        let value = serde_json::to_vec(&new_node_details).unwrap();
                                        match kvs_container.kvs.put(&key, value).await {
                                            Ok(_) => {
                                                debug!(
                                                    "change {} from a candidate to a replica",
                                                    &key
                                                );
                                            }
                                            Err(e) => {
                                                error!(
                                                    "failed to update node details: error={:?}",
                                                    e
                                                );
                                            }
                                        };
                                    }
                                }
                            }
                        }

                        // replica to primary
                        if metadata_indices.contains_key(&index_name) {
                            if metadata_indices
                                .get(&index_name)
                                .unwrap()
                                .contains_key(&shard_name)
                            {
                                let shards = metadata_indices
                                    .get(&index_name)
                                    .unwrap()
                                    .get(&shard_name)
                                    .unwrap();

                                let mut primary_exists = false;
                                for (_node_name, node_details) in shards {
                                    if node_details.state == State::Ready as i32
                                        && node_details.role == Role::Primary as i32
                                    {
                                        primary_exists = true;
                                        break;
                                    }
                                }

                                if !primary_exists {
                                    for (tmp_node_name, node_details) in shards {
                                        if node_details.state == State::Ready as i32
                                            && node_details.role == Role::Replica as i32
                                        {
                                            let mut new_node_details = node_details.clone();
                                            new_node_details.role = Role::Primary as i32;
                                            let key = format!(
                                                "/{}/{}/{}.json",
                                                &index_name, &shard_name, tmp_node_name
                                            );
                                            let value =
                                                serde_json::to_vec(&new_node_details).unwrap();
                                            match kvs_container.kvs.put(&key, value).await {
                                                Ok(_) => {
                                                    debug!(
                                                        "chage {} from a replica to a primary",
                                                        &key
                                                    );
                                                    break;
                                                }
                                                Err(e) => {
                                                    error!("failed to set: error={:?}", e);
                                                }
                                            };
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(TryRecvError::Disconnected) => {
                        debug!("channel disconnected");
                        break;
                    }
                    Err(TryRecvError::Empty) => {
                        if unreceive.load(Ordering::Relaxed) {
                            debug!("receive a stop signal");
                            // restore unreceive to false
                            unreceive.store(false, Ordering::Relaxed);
                            break;
                        }
                    }
                }
            }

            receiving.store(false, Ordering::Relaxed);
            debug!("stop cluster watch thread");
        });

        match self.probe().await {
            Ok(_) => (),
            Err(e) => return Err(anyhow!(e.to_string())),
        }

        Ok(())
    }

    pub async fn unwatch(&mut self) -> Result<()> {
        if self.kvs_container.kvs.get_type() == NOP_TYPE {
            debug!("the NOP discovery does not do anything");
            return Ok(());
        }

        if !self.receiving.load(Ordering::Relaxed) {
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

        self.unreceive.store(true, Ordering::Relaxed);

        match self.unprobe().await {
            Ok(_) => (),
            Err(e) => return Err(anyhow!(e.to_string())),
        }

        Ok(())
    }

    pub async fn probe(&mut self) -> Result<()> {
        if self.probing.load(Ordering::Relaxed) {
            let msg = "prober is already running";
            warn!("{}", &msg);
            return Err(anyhow!(msg));
        }

        let probing = Arc::clone(&self.probing);
        let unprobe = Arc::clone(&self.unprobe);
        let nodes = Arc::clone(&self.nodes);
        let probe_interval = self.probe_interval.clone();

        let kvs_container = match self.kvs_container.kvs.get_type() {
            ETCD_TYPE => {
                let config_json = self
                    .kvs_container
                    .kvs
                    .export_config_json()
                    .unwrap_or("".to_string());
                let config = match serde_json::from_str::<EtcdConfig>(config_json.as_str()) {
                    Ok(config) => config,
                    Err(e) => {
                        error!("failed to clone KVS container: error={:?}", e);
                        return Err(Error::from(e));
                    }
                };
                KVSContainer {
                    kvs: Box::new(Etcd::new(config)),
                }
            }
            _ => KVSContainer {
                kvs: Box::new(Nop::new()),
            },
        };
        // let discovery_container = self.kvs_container.clone();

        // probe nodes
        tokio::spawn(async move {
            debug!("start probe thread");
            probing.store(true, Ordering::Relaxed);

            loop {
                if unprobe.load(Ordering::Relaxed) {
                    debug!("a request to stop the prober has been received");
                    // restore stop flag to false
                    unprobe.store(false, Ordering::Relaxed);
                    break;
                }

                let mut handles: Vec<JoinHandle<State>> = Vec::new();
                let metadata_indices = nodes.read().await.clone();
                for (tmp_index_name, shards) in metadata_indices {
                    for (tmp_shard_name, nodes) in shards {
                        for (tmp_node_name, node_details) in nodes {
                            let tmp_index_name = tmp_index_name.clone();
                            let tmp_shard_name = tmp_shard_name.clone();
                            let node_details = node_details.clone();
                            let mut kvs_container = kvs_container.clone();
                            let handle = tokio::spawn(async move {
                                let grpc_server_url = format!("http://{}", node_details.address);

                                // connect
                                let mut grpc_client = match IndexServiceClient::connect(
                                    grpc_server_url.clone(),
                                )
                                .await
                                {
                                    Ok(grpc_client) => grpc_client,
                                    Err(e) => {
                                        if node_details.state != State::Disconnected as i32 {
                                            let mut new_node_details = node_details.clone();
                                            new_node_details.state = State::Disconnected as i32;
                                            new_node_details.role = Role::Candidate as i32;
                                            let key = format!(
                                                "/{}/{}/{}.json",
                                                tmp_index_name, tmp_shard_name, tmp_node_name
                                            );
                                            let value =
                                                serde_json::to_vec(&new_node_details).unwrap();
                                            error!("change {} state to disconnected", &key);
                                            match kvs_container.kvs.put(&key, value).await {
                                                Ok(_) => (),
                                                Err(e) => error!("{}", e.to_string()),
                                            };
                                        }
                                        debug!(
                                            "{} has been disconnected: {}",
                                            &grpc_server_url,
                                            e.to_string()
                                        );
                                        return State::Disconnected;
                                    }
                                };

                                // request
                                let readiness_req = tonic::Request::new(ReadinessReq {});
                                let resp = match grpc_client.readiness(readiness_req).await {
                                    Ok(resp) => resp,
                                    Err(e) => {
                                        if node_details.state != State::NotReady as i32 {
                                            let mut new_node_details = node_details.clone();
                                            new_node_details.state = State::Disconnected as i32;
                                            new_node_details.role = Role::Candidate as i32;
                                            let key = format!(
                                                "/{}/{}/{}.json",
                                                tmp_index_name, tmp_shard_name, tmp_node_name
                                            );
                                            let value =
                                                serde_json::to_vec(&new_node_details).unwrap();
                                            error!("change {} state to disconnected", &key);
                                            match kvs_container.kvs.put(&key, value).await {
                                                Ok(_) => (),
                                                Err(e) => error!("{}", e.to_string()),
                                            };
                                        }
                                        debug!(
                                            "{} has been disconnected: {}",
                                            &grpc_server_url,
                                            e.to_string()
                                        );
                                        return State::Disconnected;
                                    }
                                };

                                // check response
                                match resp.into_inner().state {
                                    state if state == State::Ready as i32 => {
                                        if node_details.state != State::Ready as i32 {
                                            let mut new_node_details = node_details.clone();
                                            new_node_details.state = State::Ready as i32;
                                            let key = format!(
                                                "/{}/{}/{}.json",
                                                tmp_index_name, tmp_shard_name, tmp_node_name
                                            );
                                            let value =
                                                serde_json::to_vec(&new_node_details).unwrap();
                                            info!("change {} state to ready", &key);
                                            match kvs_container.kvs.put(&key, value).await {
                                                Ok(_) => (),
                                                Err(e) => error!("{}", e.to_string()),
                                            };
                                        }
                                        debug!("{} has been ready", &grpc_server_url);
                                        State::Ready
                                    }
                                    _ => {
                                        if node_details.state != State::NotReady as i32 {
                                            let mut new_node_details = node_details.clone();
                                            new_node_details.state = State::NotReady as i32;
                                            new_node_details.role = Role::Candidate as i32;
                                            let key = format!(
                                                "/{}/{}/{}.json",
                                                tmp_index_name, tmp_shard_name, tmp_node_name
                                            );
                                            let value =
                                                serde_json::to_vec(&new_node_details).unwrap();
                                            warn!("change {} state to not ready", &key);
                                            match kvs_container.kvs.put(&key, value).await {
                                                Ok(_) => (),
                                                Err(e) => error!("{}", e.to_string()),
                                            };
                                        }
                                        debug!("{} has not been ready", &grpc_server_url);
                                        State::NotReady
                                    }
                                }
                            });
                            handles.push(handle);
                        }
                    }
                }

                let responses = match try_join_all(handles).await {
                    Ok(responses) => responses,
                    Err(e) => {
                        error!("failed to join handles: {}", e.to_string());
                        Vec::new()
                    }
                };

                let mut disconnected_cnt = 0;
                let mut not_ready_cnt = 0;
                let mut ready_cnt = 0;
                for response in responses {
                    match response {
                        State::Disconnected => disconnected_cnt += 1,
                        State::NotReady => not_ready_cnt += 1,
                        State::Ready => ready_cnt += 1,
                    }
                }

                debug!(
                    "disconnected:{}, not ready:{}, ready:{}",
                    disconnected_cnt, not_ready_cnt, ready_cnt
                );
                sleep(Duration::from_millis(probe_interval));
            }

            probing.store(false, Ordering::Relaxed);
            debug!("stop probe thread");
        });

        Ok(())
    }

    pub async fn unprobe(&mut self) -> Result<()> {
        if !self.probing.load(Ordering::Relaxed) {
            let msg = "prober is not running";
            warn!("{}", msg);
            return Err(anyhow!(msg));
        }

        self.unprobe.store(true, Ordering::Relaxed);

        Ok(())
    }
}
