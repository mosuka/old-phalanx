use std::collections::HashMap;
use std::error::Error;
use std::io::Error as IOError;
use std::io::ErrorKind;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::sleep;

use crossbeam::channel::{unbounded, Receiver, Sender, TryRecvError};
use lazy_static::lazy_static;
use log::*;
use regex::Regex;
use tokio::sync::RwLock;
use tokio::time::Duration;

use phalanx_discovery::discovery::etcd::{Etcd, EtcdConfig, TYPE as ETCD_TYPE};
use phalanx_discovery::discovery::nop::Nop;
use phalanx_discovery::discovery::{DiscoveryContainer, Event, EventType};
use phalanx_proto::phalanx::index_service_client::IndexServiceClient;
use phalanx_proto::phalanx::{NodeDetails, ReadinessReq, Role, State};

lazy_static! {
    static ref KEY_REGEX: Regex = Regex::new(r"^/([^/]+)/([^/]+)/([^/]+)\.json").unwrap();
}

fn parse_node_meta_key(key: &str) -> Result<(String, String, String), IOError> {
    match KEY_REGEX.captures(key) {
        Some(cap) => {
            let index_name = match cap.get(1) {
                Some(m) => m.as_str(),
                None => {
                    return Err(IOError::new(
                        ErrorKind::Other,
                        format!("index name doesn't match: key={}", key),
                    ))
                }
            };

            let shard_name = match cap.get(2) {
                Some(m) => m.as_str(),
                None => {
                    return Err(IOError::new(
                        ErrorKind::Other,
                        format!("shard name doesn't match: key={}", key),
                    ))
                }
            };

            let node_name = match cap.get(3) {
                Some(m) => m.as_str(),
                None => {
                    return Err(IOError::new(
                        ErrorKind::Other,
                        format!("node name doesn't match: key={}", key),
                    ))
                }
            };

            if node_name == "_index_meta" {
                return Err(IOError::new(
                    ErrorKind::Other,
                    format!("node name doesn't match: key={}", key),
                ));
            }

            Ok((
                index_name.to_string(),
                shard_name.to_string(),
                node_name.to_string(),
            ))
        }
        None => Err(IOError::new(
            ErrorKind::Other,
            "no match for the node meta key",
        )),
    }
}

pub struct Overseer {
    discovery_container: DiscoveryContainer,

    sender: Option<Sender<Event>>,
    receiver: Option<Receiver<Event>>,

    nodes: Arc<RwLock<HashMap<String, Option<NodeDetails>>>>,

    receiving: Arc<AtomicBool>,
    unreceive: Arc<AtomicBool>,

    probing: Arc<AtomicBool>,
    unprobe: Arc<AtomicBool>,
}

impl Overseer {
    pub fn new(discovery_container: DiscoveryContainer) -> Overseer {
        Overseer {
            discovery_container,
            sender: None,
            receiver: None,
            nodes: Arc::new(RwLock::new(HashMap::new())),
            receiving: Arc::new(AtomicBool::new(false)),
            unreceive: Arc::new(AtomicBool::new(false)),
            probing: Arc::new(AtomicBool::new(false)),
            unprobe: Arc::new(AtomicBool::new(false)),
        }
    }

    pub async fn register(
        &mut self,
        index_name: &str,
        shard_name: &str,
        node_name: &str,
        node_details: NodeDetails,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let key = format!("/{}/{}/{}.json", index_name, shard_name, node_name);
        let value = serde_json::to_string(&node_details).unwrap();

        match self.discovery_container.discovery.put(&key, &value).await {
            Ok(_) => Ok(()),
            Err(err) => {
                let msg = format!("failed to register: key={}, error={:?}", &key, err);
                error!("{}", msg);
                Err(err)
            }
        }
    }

    pub async fn unregister(
        &mut self,
        index_name: &str,
        shard_name: &str,
        node_name: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let key = format!("/{}/{}/{}.json", index_name, shard_name, node_name);

        match self.discovery_container.discovery.delete(&key).await {
            Ok(_) => Ok(()),
            Err(err) => {
                let msg = format!("failed to unregister: key={}, error={:?}", &key, err);
                error!("{}", msg);
                Err(err)
            }
        }
    }

    pub async fn inquire(
        &mut self,
        index_name: &str,
        shard_name: &str,
        node_name: &str,
    ) -> Result<Option<NodeDetails>, Box<dyn Error + Send + Sync>> {
        let key = format!("/{}/{}/{}.json", index_name, shard_name, node_name);

        match self.discovery_container.discovery.get(&key).await {
            Ok(response) => match response {
                Some(json) => match serde_json::from_str::<NodeDetails>(json.as_str()) {
                    Ok(node_details) => Ok(Some(node_details)),
                    Err(err) => {
                        let msg = format!(
                            "failed to parse node details: key={}, error={:?}",
                            &key, err
                        );
                        error!("{}", &msg);
                        Err(Box::new(IOError::new(ErrorKind::Other, msg)))
                    }
                },
                None => {
                    let msg = format!("not found: key={}", &key);
                    debug!("{}", msg);
                    Ok(None)
                }
            },
            Err(err) => {
                let msg = format!("failed to inquire: key={}, error={:?}", &key, err);
                error!("{}", msg);
                Err(err)
            }
        }
    }

    pub async fn watch(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        // initialize sender and receiver
        let (sender, receiver) = unbounded();
        self.sender = Some(sender);
        self.receiver = Some(receiver);

        // prepare sender
        let sender = self.sender.as_ref().unwrap().clone();

        let key = "/";

        match self.discovery_container.discovery.watch(sender, key).await {
            Ok(_) => Ok(()),
            Err(err) => {
                let msg = format!("failed to watch: key={}, error={:?}", key, err);
                error!("{}", msg);
                Err(err)
            }
        }
    }

    pub async fn unwatch(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        match self.discovery_container.discovery.unwatch().await {
            Ok(_) => Ok(()),
            Err(err) => {
                let msg = format!("failed to unwatch: error={:?}", err);
                error!("{}", msg);
                Err(err)
            }
        }
    }

    pub async fn receive(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        if self.receiving.load(Ordering::Relaxed) {
            let msg = "receiver is already running";
            warn!("{}", msg);
            return Err(Box::new(IOError::new(ErrorKind::Other, msg)));
        }

        self.nodes = Arc::new(RwLock::new(HashMap::new()));
        let nodes = Arc::clone(&self.nodes);

        // prepare receiver
        let receiver = self.receiver.as_ref().unwrap().clone();
        let receiving = Arc::clone(&self.receiving);
        let unreceive = Arc::clone(&self.unreceive);

        let config_json = self
            .discovery_container
            .discovery
            .export_config_json()
            .unwrap_or("".to_string());
        let discovery_container = match self.discovery_container.discovery.get_type() {
            ETCD_TYPE => {
                let config = match serde_json::from_str::<EtcdConfig>(config_json.as_str()) {
                    Ok(config) => config,
                    Err(err) => {
                        let msg = format!("failed to  parse config JSON: error={:?}", err);
                        error!("{}", msg);
                        return Err(Box::new(IOError::new(ErrorKind::Other, msg)));
                    }
                };
                DiscoveryContainer {
                    discovery: Box::new(Etcd::new(config)),
                }
            }
            _ => DiscoveryContainer {
                discovery: Box::new(Nop::new()),
            },
        };
        // let discovery_container = self.discovery_container.clone();

        tokio::spawn(async move {
            info!("start receive thread");
            receiving.store(true, Ordering::Relaxed);

            {
                let mut discovery_container = discovery_container.clone();

                debug!("initialize nodes cache");
                let key = "/";
                match discovery_container.discovery.list(key).await {
                    Ok(kvps) => {
                        for kvp in kvps {
                            // check whether a key is a node meta data key
                            match parse_node_meta_key(&kvp.key) {
                                Ok(_) => (),
                                Err(e) => {
                                    // ignore keys that do not match the pattern
                                    debug!("node meta key parse error: error={:?}", e);
                                    continue;
                                }
                            };

                            // parse JSON to create node metadata
                            let node_details = match kvp.value {
                                Some(value) => {
                                    match serde_json::from_str::<NodeDetails>(value.as_str()) {
                                        Ok(node_details) => node_details,
                                        Err(err) => {
                                            error!("failed to parse JSON: error={:?}", err);
                                            continue;
                                        }
                                    }
                                }
                                None => {
                                    warn!("empty data: key value pair={:?}", kvp);
                                    continue;
                                }
                            };

                            // add node metadata to local cache
                            info!(
                                "set local node cash: key={}, node_details={:?}",
                                &kvp.key, &node_details
                            );
                            let mut n = nodes.write().await;
                            n.insert(kvp.key, Some(node_details));
                        }
                    }
                    Err(err) => {
                        error!("failed to list: error={:?}", err);
                    }
                };
                debug!("nodes cache has been initialized");
            }

            loop {
                let mut discovery_container = discovery_container.clone();

                match receiver.try_recv() {
                    Ok(event) => {
                        let (index_name, shard_name, _node_name) =
                            match parse_node_meta_key(&event.key) {
                                Ok(result) => result,
                                Err(e) => {
                                    // ignore keys that do not match the pattern
                                    debug!("node meta key parse error: error={:?}", e);
                                    continue;
                                }
                            };

                        debug!("update local node cache");
                        match event.event_type {
                            EventType::Put => {
                                let node_details =
                                    match serde_json::from_str::<NodeDetails>(event.value.as_str())
                                    {
                                        Ok(node_details) => node_details,
                                        Err(err) => {
                                            error!("failed to parse JSON: error={:?}", err);
                                            continue;
                                        }
                                    };

                                info!(
                                    "set local node cash: key={}, node_details={:?}",
                                    &event.key, &node_details
                                );
                                let mut n = nodes.write().await;
                                n.insert(event.key.to_string(), Some(node_details));
                            }
                            EventType::Delete => {
                                info!("delete local node cash: key={}", &event.key);
                                let mut n = nodes.write().await;
                                n.remove(event.key.as_str());
                            }
                        };
                        debug!("local node cache has been updated");

                        debug!("make a ready candidate node an replica node");
                        let sel_key = format!("/{}/{}/", index_name, shard_name);
                        debug!("check the nodes under {}", &sel_key);
                        match discovery_container.discovery.list(sel_key.as_str()).await {
                            Ok(kvps) => {
                                for kvp in kvps {
                                    match parse_node_meta_key(&kvp.key) {
                                        Ok(_) => (),
                                        Err(e) => {
                                            // ignore keys that do not match the pattern
                                            debug!("node meta key parse error: error={:?}", e);
                                            continue;
                                        }
                                    }

                                    debug!("check: key={}", &kvp.key);
                                    let mut node_details = match kvp.value {
                                        Some(value) => {
                                            match serde_json::from_str::<NodeDetails>(
                                                value.as_str(),
                                            ) {
                                                Ok(node_details) => node_details,
                                                Err(e) => {
                                                    error!("failed to parse JSON: error={:?}", e);
                                                    continue;
                                                }
                                            }
                                        }
                                        None => {
                                            error!("value is empty");
                                            continue;
                                        }
                                    };

                                    if node_details.state == State::Ready as i32
                                        && node_details.role == Role::Candidate as i32
                                    {
                                        node_details.role = Role::Replica as i32;
                                        let value = serde_json::to_string(&node_details).unwrap();
                                        match discovery_container
                                            .discovery
                                            .put(kvp.key.as_str(), value.as_str())
                                            .await
                                        {
                                            Ok(_) => {
                                                info!(
                                                    "put node metadata: key={}, value={}",
                                                    &kvp.key, &value
                                                );
                                            }
                                            Err(e) => {
                                                error!(
                                                    "failed to update node details: error={:?}",
                                                    e
                                                );
                                            }
                                        };
                                    } else {
                                        debug!(
                                            "{} is not a node to be updated: node_details={:?}",
                                            &kvp.key, &node_details
                                        );
                                        continue;
                                    }
                                }
                            }
                            Err(e) => {
                                error!("failed to list: error={:?}", e);
                            }
                        };
                        debug!("the ready candidate nodes have been made into replica nodes");

                        debug!("make a ready replica node a primary node");
                        match discovery_container.discovery.list(sel_key.as_str()).await {
                            Ok(kvps) => {
                                let mut primary_exists = false;

                                debug!(
                                    "check whether the primary node already exists under {}",
                                    &sel_key
                                );
                                for kvp in kvps.iter() {
                                    match parse_node_meta_key(&kvp.key) {
                                        Ok(_) => (),
                                        Err(e) => {
                                            // ignore keys that do not match the pattern
                                            debug!("node meta key parse error: error={:?}", e);
                                            continue;
                                        }
                                    };

                                    match &kvp.value {
                                        Some(value) => {
                                            match serde_json::from_str::<NodeDetails>(
                                                value.as_str(),
                                            ) {
                                                Ok(node_details) => {
                                                    if node_details.state == State::Ready as i32
                                                        && node_details.role == Role::Primary as i32
                                                    {
                                                        primary_exists = true;
                                                    }
                                                }
                                                Err(e) => {
                                                    error!("failed to parse JSON: error={:?}", e);
                                                }
                                            }
                                        }
                                        None => {
                                            error!("value is empty");
                                        }
                                    };
                                }
                                debug!(
                                    "checked whether the primary node already exists under {}",
                                    &sel_key
                                );

                                if !primary_exists {
                                    info!("there is no primary node under {}", sel_key);
                                    for kvp in kvps.iter() {
                                        match parse_node_meta_key(&kvp.key) {
                                            Ok(_) => (),
                                            Err(e) => {
                                                // ignore keys that do not match the pattern
                                                debug!("node meta key parse error: error={:?}", e);
                                                continue;
                                            }
                                        };

                                        debug!("check: key={}", &kvp.key);
                                        let mut node_details = match &kvp.value {
                                            Some(value) => {
                                                match serde_json::from_str::<NodeDetails>(
                                                    value.as_str(),
                                                ) {
                                                    Ok(node_details) => node_details,
                                                    Err(e) => {
                                                        error!(
                                                            "failed to parse JSON: error={:?}",
                                                            e
                                                        );
                                                        continue;
                                                    }
                                                }
                                            }
                                            None => {
                                                error!("value is empty");
                                                continue;
                                            }
                                        };

                                        if node_details.state == State::Ready as i32
                                            && node_details.role == Role::Replica as i32
                                        {
                                            node_details.role = Role::Primary as i32;
                                            let value =
                                                serde_json::to_string(&node_details).unwrap();
                                            match discovery_container
                                                .discovery
                                                .put(&kvp.key, value.as_str())
                                                .await
                                            {
                                                Ok(_) => {
                                                    info!(
                                                        "put node metadata: key={}, value={}",
                                                        &kvp.key, &value
                                                    );
                                                    break;
                                                }
                                                Err(e) => {
                                                    error!("failed to set: error={:?}", e);
                                                }
                                            };
                                        } else {
                                            debug!(
                                                "{} is not a node to be updated: node_details={:?}",
                                                &kvp.key, &node_details
                                            );
                                            continue;
                                        }
                                    }
                                } else {
                                    info!("the primary node exists under {}", &sel_key);
                                }
                            }
                            Err(e) => {
                                error!("failed to list: error={:?}", e);
                            }
                        };
                    }
                    Err(TryRecvError::Disconnected) => {
                        info!("channel disconnected");
                        break;
                    }
                    Err(TryRecvError::Empty) => {
                        if unreceive.load(Ordering::Relaxed) {
                            info!("receive a stop signal");
                            // restore unreceive to false
                            unreceive.store(false, Ordering::Relaxed);
                            break;
                        }
                    }
                };
            }

            receiving.store(false, Ordering::Relaxed);
            info!("stop receive thread");
        });

        Ok(())
    }

    pub async fn unreceive(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        if !self.receiving.load(Ordering::Relaxed) {
            let msg = "receiver is not running";
            warn!("{}", msg);
            return Err(Box::new(IOError::new(ErrorKind::Other, msg)));
        }

        self.unreceive.store(true, Ordering::Relaxed);

        Ok(())
    }

    pub async fn probe(&mut self, interval: u64) -> Result<(), Box<dyn Error + Send + Sync>> {
        if self.probing.load(Ordering::Relaxed) {
            let msg = "prober is already running";
            warn!("{}", msg);
            return Err(Box::new(IOError::new(ErrorKind::Other, msg)));
        }

        let probing = Arc::clone(&self.probing);
        let unprobe = Arc::clone(&self.unprobe);
        let nodes = Arc::clone(&self.nodes);

        let config_json = self
            .discovery_container
            .discovery
            .export_config_json()
            .unwrap_or("".to_string());
        let discovery_container = match self.discovery_container.discovery.get_type() {
            ETCD_TYPE => {
                let config = match serde_json::from_str::<EtcdConfig>(config_json.as_str()) {
                    Ok(config) => config,
                    Err(e) => {
                        return Err(Box::new(IOError::new(
                            ErrorKind::Other,
                            format!("failed to parse config JSON: error={:?}", e),
                        )));
                    }
                };
                DiscoveryContainer {
                    discovery: Box::new(Etcd::new(config)),
                }
            }
            _ => DiscoveryContainer {
                discovery: Box::new(Nop::new()),
            },
        };
        // let discovery_container = self.discovery_container.clone();

        tokio::spawn(async move {
            info!("start probe thread");
            probing.store(true, Ordering::Relaxed);

            loop {
                let mut discovery_container = discovery_container.clone();

                if unprobe.load(Ordering::Relaxed) {
                    debug!("a request to stop the prober has been received");

                    // restore stop flag to false
                    unprobe.store(false, Ordering::Relaxed);
                    break;
                } else {
                    let nodes = nodes.read().await;
                    for (key, value) in nodes.iter() {
                        let node_details = match value {
                            Some(node_details) => node_details,
                            None => {
                                error!("value is empty");
                                continue;
                            }
                        };

                        let grpc_server_url = format!("http://{}", node_details.address);

                        debug!("connect: {}", grpc_server_url);
                        let mut grpc_client =
                            match IndexServiceClient::connect(grpc_server_url.clone()).await {
                                Ok(grpc_client) => grpc_client,
                                Err(_e) => {
                                    if node_details.state != State::Disconnected as i32 {
                                        info!("disconnected: {}", &grpc_server_url);
                                        let new_node_details = NodeDetails {
                                            address: node_details.address.clone(),
                                            state: State::Disconnected as i32,
                                            role: Role::Candidate as i32,
                                        };
                                        let value =
                                            serde_json::to_string(&new_node_details).unwrap();
                                        match discovery_container
                                            .discovery
                                            .put(key.as_str(), value.as_str())
                                            .await
                                        {
                                            Ok(_) => {
                                                info!(
                                                    "put node metadata: key={}, value={}",
                                                    key, &value
                                                );
                                            }
                                            Err(e) => {
                                                error!("failed to put: error={:?}", e);
                                            }
                                        };
                                    } else {
                                        // still disconnected
                                        debug!("disconnected: {}", &grpc_server_url);
                                    }

                                    continue;
                                }
                            };

                        debug!("readiness check: {}", &grpc_server_url);
                        let readiness_req = tonic::Request::new(ReadinessReq {});
                        let resp = match grpc_client.readiness(readiness_req).await {
                            Ok(resp) => resp,
                            Err(_e) => {
                                if node_details.state != State::NotReady as i32 {
                                    info!("not ready: {}", &grpc_server_url);
                                    let new_node_details = NodeDetails {
                                        address: node_details.address.clone(),
                                        state: State::NotReady as i32,
                                        role: Role::Candidate as i32,
                                    };
                                    let value = serde_json::to_string(&new_node_details).unwrap();
                                    match discovery_container
                                        .discovery
                                        .put(key.as_str(), value.as_str())
                                        .await
                                    {
                                        Ok(_) => {
                                            info!(
                                                "put node metadata: key={}, value={}",
                                                key, &value
                                            );
                                        }
                                        Err(e) => {
                                            error!("failed to set: error={:?}", e);
                                        }
                                    };
                                } else {
                                    // still not ready
                                    debug!("not ready: {}", &grpc_server_url);
                                }

                                continue;
                            }
                        };

                        debug!("check readiness response: {}", &grpc_server_url);
                        match resp.into_inner().state {
                            state if state == State::Ready as i32 => {
                                if node_details.state != State::Ready as i32 {
                                    info!("ready: {}", &grpc_server_url);
                                    let new_node_details = NodeDetails {
                                        address: node_details.address.clone(),
                                        state: State::Ready as i32,
                                        role: node_details.role,
                                    };
                                    let value = serde_json::to_string(&new_node_details).unwrap();
                                    match discovery_container
                                        .discovery
                                        .put(key.as_str(), value.as_str())
                                        .await
                                    {
                                        Ok(_) => {
                                            info!(
                                                "put node metadata: key={}, value={}",
                                                key, &value
                                            );
                                        }
                                        Err(e) => {
                                            error!("failed to set: error={:?}", e);
                                        }
                                    };
                                } else {
                                    // no state changes
                                    debug!("ready: {}", &grpc_server_url);
                                }
                            }
                            _ => {
                                if node_details.state != State::NotReady as i32 {
                                    info!("not ready: {}", &grpc_server_url);
                                    let new_node_details = NodeDetails {
                                        address: node_details.address.clone(),
                                        state: State::NotReady as i32,
                                        role: Role::Candidate as i32,
                                    };
                                    let value = serde_json::to_string(&new_node_details).unwrap();
                                    match discovery_container
                                        .discovery
                                        .put(key.as_str(), value.as_str())
                                        .await
                                    {
                                        Ok(_) => {
                                            info!(
                                                "put node metadata: key={}, value={}",
                                                key, &value
                                            );
                                        }
                                        Err(e) => {
                                            error!("failed to set: error={:?}", e);
                                        }
                                    };
                                } else {
                                    // still not ready
                                    debug!("not ready: {}", &grpc_server_url);
                                }
                            }
                        }
                    }
                }

                sleep(Duration::from_millis(interval));
            }

            probing.store(false, Ordering::Relaxed);
            info!("stop probe thread");
        });

        Ok(())
    }

    pub async fn unprobe(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        if !self.probing.load(Ordering::Relaxed) {
            let msg = "prober is not running";
            warn!("{}", msg);
            return Err(Box::new(IOError::new(ErrorKind::Other, msg)));
        }

        self.unprobe.store(true, Ordering::Relaxed);

        Ok(())
    }
}
