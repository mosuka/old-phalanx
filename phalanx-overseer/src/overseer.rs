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

use phalanx_kvs::kvs::etcd::{Etcd, EtcdConfig, TYPE as ETCD_TYPE};
use phalanx_kvs::kvs::nop::Nop;
use phalanx_kvs::kvs::{Event, EventType, KVSContainer};
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
    kvs_container: KVSContainer,

    sender: Option<Sender<Event>>,
    receiver: Option<Receiver<Event>>,

    nodes: Arc<RwLock<HashMap<String, Option<NodeDetails>>>>,

    receiving: Arc<AtomicBool>,
    unreceive: Arc<AtomicBool>,

    probing: Arc<AtomicBool>,
    unprobe: Arc<AtomicBool>,
}

impl Overseer {
    pub fn new(kvs_container: KVSContainer) -> Overseer {
        Overseer {
            kvs_container,
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
        let value = serde_json::to_vec(&node_details).unwrap();

        match self.kvs_container.kvs.put(&key, value).await {
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

        match self.kvs_container.kvs.delete(&key).await {
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

        match self.kvs_container.kvs.get(&key).await {
            Ok(response) => match response {
                Some(json) => match serde_json::from_slice::<NodeDetails>(json.as_slice()) {
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

        match self.kvs_container.kvs.watch(sender, key).await {
            Ok(_) => Ok(()),
            Err(err) => {
                let msg = format!("failed to watch: key={}, error={:?}", key, err);
                error!("{}", msg);
                Err(err)
            }
        }
    }

    pub async fn unwatch(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        match self.kvs_container.kvs.unwatch().await {
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
            .kvs_container
            .kvs
            .export_config_json()
            .unwrap_or("".to_string());
        let kvs_container = match self.kvs_container.kvs.get_type() {
            ETCD_TYPE => {
                let config = match serde_json::from_str::<EtcdConfig>(config_json.as_str()) {
                    Ok(config) => config,
                    Err(err) => {
                        let msg = format!("failed to  parse config JSON: error={:?}", err);
                        error!("{}", msg);
                        return Err(Box::new(IOError::new(ErrorKind::Other, msg)));
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
        // let discovery_container = self.discovery_container.clone();

        tokio::spawn(async move {
            debug!("start receive thread");
            receiving.store(true, Ordering::Relaxed);

            {
                let mut kvs_container = kvs_container.clone();

                // initialize nodes cache
                let key = "/";
                match kvs_container.kvs.list(key).await {
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
                            let node_details =
                                match serde_json::from_slice::<NodeDetails>(kvp.value.as_slice()) {
                                    Ok(node_details) => node_details,
                                    Err(err) => {
                                        error!("failed to parse JSON: error={:?}", err);
                                        continue;
                                    }
                                };

                            // add node metadata to local cache
                            debug!("{} has been set to local cache", &kvp.key);
                            let mut n = nodes.write().await;
                            n.insert(kvp.key, Some(node_details));
                        }
                    }
                    Err(err) => {
                        error!("failed to list: error={:?}", err);
                    }
                };
            }

            loop {
                let mut kvs_container = kvs_container.clone();

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

                        match event.event_type {
                            EventType::Put => {
                                let node_details = match serde_json::from_slice::<NodeDetails>(
                                    event.value.as_slice(),
                                ) {
                                    Ok(node_details) => node_details,
                                    Err(err) => {
                                        error!("failed to parse JSON: error={:?}", err);
                                        continue;
                                    }
                                };

                                debug!("{} has been set to local cache", &event.key);
                                let mut n = nodes.write().await;
                                n.insert(event.key.to_string(), Some(node_details));
                            }
                            EventType::Delete => {
                                debug!("delete local node cash: key={}", &event.key);
                                let mut n = nodes.write().await;
                                n.remove(event.key.as_str());
                            }
                        };

                        let sel_key = format!("/{}/{}/", index_name, shard_name);
                        match kvs_container.kvs.list(sel_key.as_str()).await {
                            Ok(kvps) => {
                                for kvp in kvps {
                                    match parse_node_meta_key(&kvp.key) {
                                        Ok(_) => (),
                                        Err(_e) => {
                                            // ignore keys that do not match the pattern
                                            continue;
                                        }
                                    }

                                    let mut node_details = match serde_json::from_slice::<NodeDetails>(
                                        kvp.value.as_slice(),
                                    ) {
                                        Ok(node_details) => node_details,
                                        Err(e) => {
                                            error!("failed to parse JSON: error={:?}", e);
                                            continue;
                                        }
                                    };

                                    if node_details.state == State::Ready as i32
                                        && node_details.role == Role::Candidate as i32
                                    {
                                        node_details.role = Role::Replica as i32;
                                        let value = serde_json::to_vec(&node_details).unwrap();
                                        match kvs_container.kvs.put(kvp.key.as_str(), value).await {
                                            Ok(_) => {
                                                debug!(
                                                    "{} has changed from a candidate to a replica",
                                                    &kvp.key
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
                            Err(e) => {
                                error!("failed to list: error={:?}", e);
                            }
                        };

                        match kvs_container.kvs.list(sel_key.as_str()).await {
                            Ok(kvps) => {
                                let mut primary_exists = false;

                                for kvp in kvps.iter() {
                                    match parse_node_meta_key(&kvp.key) {
                                        Ok(_) => (),
                                        Err(_e) => {
                                            // ignore keys that do not match the pattern
                                            continue;
                                        }
                                    };

                                    match serde_json::from_slice::<NodeDetails>(
                                        kvp.value.as_slice(),
                                    ) {
                                        Ok(node_details) => {
                                            if node_details.state == State::Ready as i32
                                                && node_details.role == Role::Primary as i32
                                            {
                                                primary_exists = true;
                                                break;
                                            }
                                        }
                                        Err(e) => {
                                            error!("failed to parse JSON: error={:?}", e);
                                        }
                                    };
                                }

                                if !primary_exists {
                                    for kvp in kvps.iter() {
                                        match parse_node_meta_key(&kvp.key) {
                                            Ok(_) => (),
                                            Err(_e) => {
                                                // ignore keys that do not match the pattern
                                                continue;
                                            }
                                        };

                                        let mut node_details =
                                            match serde_json::from_slice::<NodeDetails>(
                                                &kvp.value.as_slice(),
                                            ) {
                                                Ok(node_details) => node_details,
                                                Err(e) => {
                                                    error!("failed to parse JSON: error={:?}", e);
                                                    continue;
                                                }
                                            };

                                        if node_details.state == State::Ready as i32
                                            && node_details.role == Role::Replica as i32
                                        {
                                            node_details.role = Role::Primary as i32;
                                            let value = serde_json::to_vec(&node_details).unwrap();
                                            match kvs_container.kvs.put(&kvp.key, value).await {
                                                Ok(_) => {
                                                    debug!(
                                                        "{} has changed from a replica to a primary",
                                                        &kvp.key
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
                            Err(e) => {
                                error!("failed to list: error={:?}", e);
                            }
                        };
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
                };
            }

            receiving.store(false, Ordering::Relaxed);
            debug!("stop receive thread");
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
            .kvs_container
            .kvs
            .export_config_json()
            .unwrap_or("".to_string());
        let kvs_container = match self.kvs_container.kvs.get_type() {
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
                KVSContainer {
                    kvs: Box::new(Etcd::new(config)),
                }
            }
            _ => KVSContainer {
                kvs: Box::new(Nop::new()),
            },
        };
        // let discovery_container = self.discovery_container.clone();

        tokio::spawn(async move {
            debug!("start probe thread");
            probing.store(true, Ordering::Relaxed);

            loop {
                let mut kvs_container = kvs_container.clone();

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
                        let mut grpc_client =
                            match IndexServiceClient::connect(grpc_server_url.clone()).await {
                                Ok(grpc_client) => grpc_client,
                                Err(_e) => {
                                    if node_details.state != State::Disconnected as i32 {
                                        error!("{} has been disconnected", key);
                                        let new_node_details = NodeDetails {
                                            address: node_details.address.clone(),
                                            state: State::Disconnected as i32,
                                            role: Role::Candidate as i32,
                                        };
                                        let value = serde_json::to_vec(&new_node_details).unwrap();
                                        match kvs_container.kvs.put(key.as_str(), value).await {
                                            Ok(_) => (),
                                            Err(e) => {
                                                error!("failed to put: error={:?}", e);
                                            }
                                        };
                                    } else {
                                        // still disconnected
                                        debug!("{} has been disconnected", key);
                                    }

                                    continue;
                                }
                            };

                        let readiness_req = tonic::Request::new(ReadinessReq {});
                        let resp = match grpc_client.readiness(readiness_req).await {
                            Ok(resp) => resp,
                            Err(_e) => {
                                if node_details.state != State::NotReady as i32 {
                                    warn!("{} is not ready", key);
                                    let new_node_details = NodeDetails {
                                        address: node_details.address.clone(),
                                        state: State::NotReady as i32,
                                        role: Role::Candidate as i32,
                                    };
                                    let value = serde_json::to_vec(&new_node_details).unwrap();
                                    match kvs_container.kvs.put(key.as_str(), value).await {
                                        Ok(_) => (),
                                        Err(e) => {
                                            error!("failed to set: error={:?}", e);
                                        }
                                    };
                                } else {
                                    // still not ready
                                    debug!("{} is not ready", key);
                                }

                                continue;
                            }
                        };

                        match resp.into_inner().state {
                            state if state == State::Ready as i32 => {
                                if node_details.state != State::Ready as i32 {
                                    info!("{} is ready", key);
                                    let new_node_details = NodeDetails {
                                        address: node_details.address.clone(),
                                        state: State::Ready as i32,
                                        role: node_details.role,
                                    };
                                    let value = serde_json::to_vec(&new_node_details).unwrap();
                                    match kvs_container.kvs.put(key.as_str(), value).await {
                                        Ok(_) => (),
                                        Err(e) => {
                                            error!("failed to set: error={:?}", e);
                                        }
                                    };
                                } else {
                                    // no state changes
                                    debug!("{} is ready", key);
                                }
                            }
                            _ => {
                                if node_details.state != State::NotReady as i32 {
                                    warn!("{} is not ready", key);
                                    let new_node_details = NodeDetails {
                                        address: node_details.address.clone(),
                                        state: State::NotReady as i32,
                                        role: Role::Candidate as i32,
                                    };
                                    let value = serde_json::to_vec(&new_node_details).unwrap();
                                    match kvs_container.kvs.put(key.as_str(), value).await {
                                        Ok(_) => (),
                                        Err(e) => {
                                            error!("failed to set: error={:?}", e);
                                        }
                                    };
                                } else {
                                    // still not ready
                                    debug!("{} is not ready", key);
                                }
                            }
                        }
                    }
                }

                sleep(Duration::from_millis(interval));
            }

            probing.store(false, Ordering::Relaxed);
            debug!("stop probe thread");
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
