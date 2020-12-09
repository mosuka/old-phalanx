use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crossbeam::channel::{unbounded, TryRecvError};
use futures::future::try_join_all;
use lazy_static::lazy_static;
use log::*;
use rand::Rng;
use regex::Regex;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tonic::transport::Channel;

use phalanx_common::error::{Error, ErrorKind};
use phalanx_discovery::discovery::nop::TYPE as NOP_TYPE;
use phalanx_discovery::discovery::{DiscoveryContainer, EventType};
use phalanx_proto::phalanx::index_service_client::IndexServiceClient;
use phalanx_proto::phalanx::{GetReq, NodeDetails, ReadinessReq, Role, State};
use tonic::Code;

const KEY_PATTERN: &'static str = r"^/([^/]+)/([^/]+)/([^/]+)\.json";

lazy_static! {
    static ref KEY_REGEX: Regex = Regex::new(KEY_PATTERN).unwrap();
}

fn parse_key(key: &str) -> Result<(String, String, String), Error> {
    match KEY_REGEX.captures(key) {
        Some(cap) => {
            let value1 = match cap.get(1) {
                Some(m) => m.as_str(),
                None => {
                    return Err(Error::from(ErrorKind::NotFound(String::from(
                        "value does not match",
                    ))));
                }
            };

            let value2 = match cap.get(2) {
                Some(m) => m.as_str(),
                None => {
                    return Err(Error::from(ErrorKind::NotFound(String::from(
                        "value does not match",
                    ))));
                }
            };

            let value3 = match cap.get(3) {
                Some(m) => m.as_str(),
                None => {
                    return Err(Error::from(ErrorKind::NotFound(String::from(
                        "value does not match",
                    ))));
                }
            };

            Ok((value1.to_string(), value2.to_string(), value3.to_string()))
        }
        None => Err(Error::from(ErrorKind::ParseError(format!(
            "{:?} does not match {:?}",
            key, KEY_PATTERN
        )))),
    }
}

#[derive(Clone)]
pub struct Client {
    discovery_container: DiscoveryContainer,
    watching: Arc<AtomicBool>,
    unwatch: Arc<AtomicBool>,
    node_metadata: Arc<RwLock<HashMap<String, NodeDetails>>>,
    clients: Arc<RwLock<HashMap<String, IndexServiceClient<Channel>>>>,
}

impl Client {
    pub async fn new(discovery_container: DiscoveryContainer) -> Client {
        Client {
            discovery_container,
            watching: Arc::new(AtomicBool::new(false)),
            unwatch: Arc::new(AtomicBool::new(false)),
            node_metadata: Arc::new(RwLock::new(HashMap::new())),
            clients: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn watch(&mut self, key: &str) -> Result<(), Error> {
        if self.discovery_container.discovery.get_type() == NOP_TYPE {
            debug!("the NOP discovery does not do anything");
            return Ok(());
        }

        if self.watching.load(Ordering::Relaxed) {
            let msg = "receiver is already running";
            warn!("{}", msg);
            return Err(Error::from(ErrorKind::Other(msg.to_string())));
        }

        // initialize
        self.node_metadata = Arc::new(RwLock::new(HashMap::new()));
        self.clients = Arc::new(RwLock::new(HashMap::new()));
        let (sender, receiver) = unbounded();

        // specifies the key of the shard to which it joined
        match self.discovery_container.discovery.watch(sender, key).await {
            Ok(_) => (),
            Err(e) => {
                return Err(Error::from(ErrorKind::Other(e.to_string())));
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
                    let names = match parse_key(&kvp.key) {
                        Ok(names) => names,
                        Err(e) => {
                            // ignore keys that do not match the pattern
                            error!("{}", e.to_string());
                            continue;
                        }
                    };

                    if names.2 == "_index_meta" {
                        // skip index metadata
                        continue;
                    }

                    // make metadata
                    let nd = match serde_json::from_slice::<NodeDetails>(kvp.value.as_slice()) {
                        Ok(node_details) => node_details,
                        Err(err) => {
                            error!("failed to parse JSON: error={:?}", err);
                            continue;
                        }
                    };

                    // add client
                    let addr = format!("http://{}", nd.address.clone());
                    match IndexServiceClient::connect(addr).await {
                        Ok(client) => {
                            debug!("add {} in the client pool", &kvp.key);
                            let mut c = self.clients.write().await;
                            c.insert(kvp.key.clone(), client);
                        }
                        Err(e) => {
                            error!("failed to connect to {}: {}", &nd.address, e.to_string());
                        }
                    };

                    // add metadata
                    let mut nm = self.node_metadata.write().await;
                    nm.insert(kvp.key.clone(), nd);
                }
            }
            Err(err) => {
                error!("failed to list: error={:?}", err);
            }
        };

        // let cache = Arc::clone(&self.cache);
        let node_metadata = Arc::clone(&self.node_metadata);
        let clients = Arc::clone(&self.clients);

        tokio::spawn(async move {
            debug!("start cluster watch thread");
            watching.store(true, Ordering::Relaxed);

            loop {
                match receiver.try_recv() {
                    Ok(event) => {
                        // check key format
                        let names = match parse_key(&event.key) {
                            Ok(names) => names,
                            Err(e) => {
                                // ignore keys that do not match the pattern
                                debug!("parse error: error={:?}", e);
                                continue;
                            }
                        };

                        if names.2 == "_index_meta" {
                            // skip index metadata
                            continue;
                        }

                        // make metadata
                        let nd = match serde_json::from_slice::<NodeDetails>(event.value.as_slice())
                        {
                            Ok(node_details) => node_details,
                            Err(err) => {
                                error!("failed to parse JSON: error={:?}", err);
                                continue;
                            }
                        };

                        match event.event_type {
                            EventType::Put => {
                                // add client
                                let addr = format!("http://{}", nd.address.clone());
                                match IndexServiceClient::connect(addr).await {
                                    Ok(client) => {
                                        debug!("add {} in the client pool", &event.key);
                                        let mut c = clients.write().await;
                                        c.insert(event.key.clone(), client);
                                    }
                                    Err(e) => {
                                        error!(
                                            "failed to connect to {}: {}",
                                            &nd.address,
                                            e.to_string()
                                        );
                                        let mut c = clients.write().await;
                                        if c.contains_key(&event.key) {
                                            c.remove(&event.key);
                                        }
                                    }
                                };

                                // add node metadata
                                let mut nm = node_metadata.write().await;
                                nm.insert(event.key.clone(), nd);
                            }
                            EventType::Delete => {
                                // delete client
                                debug!("delete client: key={}", &event.key);
                                let mut c = clients.write().await;
                                if c.contains_key(&event.key) {
                                    c.remove(&event.key);
                                }

                                // delete node metadata
                                debug!("delete node metadata: key={}", &event.key);
                                let mut nm = node_metadata.write().await;
                                if nm.contains_key(&event.key) {
                                    nm.remove(&event.key);
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
            return Err(Error::from(ErrorKind::Other(msg.to_string())));
        }

        match self.discovery_container.discovery.unwatch().await {
            Ok(_) => (),
            Err(e) => {
                return Err(Error::from(ErrorKind::Other(e.to_string())));
            }
        }

        self.unwatch.store(true, Ordering::Relaxed);

        Ok(())
    }

    pub async fn readiness(
        &mut self,
        index_name: &str,
        shard_name: Option<&str>,
        node_name: Option<&str>,
    ) -> Result<HashMap<String, State>, Error> {
        let keys: Vec<String> = {
            let mut prefix = format!("/{}/", index_name);
            match shard_name {
                Some(shard_name) => {
                    prefix.push_str(shard_name);
                    prefix.push_str("/");
                    match node_name {
                        Some(node_name) => {
                            prefix.push_str(node_name);
                            prefix.push_str(".json");
                        }
                        None => (),
                    };
                }
                None => (),
            };

            let mut keys = Vec::new();
            for key in self.node_metadata.read().await.keys() {
                if key.starts_with(prefix.as_str()) {
                    keys.push(key.to_string());
                }
            }
            keys
        };

        let clients = self.clients.read().await;
        let handles: Vec<JoinHandle<Result<State, Error>>> = keys
            .iter()
            .map(|key| {
                let key = key.clone();
                match clients.get(&key) {
                    Some(client) => {
                        let client = client.clone();
                        tokio::spawn(async move {
                            let mut client: IndexServiceClient<Channel> = client.into();
                            let req = tonic::Request::new(ReadinessReq {});
                            match client.readiness(req).await {
                                Ok(resp) => {
                                    let state = resp.into_inner().state;
                                    match State::from_i32(state) {
                                        Some(state) => Ok(state),
                                        None => {
                                            error!("unknown state {}", state);
                                            Ok(State::Disconnected)
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!("{}", e.to_string());
                                    Ok(State::Disconnected)
                                }
                            }
                        })
                    }
                    None => tokio::spawn(async move {
                        error!("client for {} does not exist", key);
                        Ok(State::Disconnected)
                    }),
                }
            })
            .collect::<Vec<_>>();

        let states: Vec<State> = try_join_all(handles)
            .await
            .unwrap()
            .into_iter()
            .collect::<Result<Vec<State>, Error>>()
            .unwrap();

        let mut state_hashmap = HashMap::new();
        for (key, state) in keys.iter().zip(states) {
            state_hashmap.insert(key.clone(), state);
        }

        Ok(state_hashmap)
    }

    pub async fn get(
        &mut self,
        id: &str,
        index_name: &str,
        shard_name: Option<&str>,
        node_name: Option<&str>,
    ) -> Result<Vec<u8>, Error> {
        let mut prefix = format!("/{}/", index_name);
        match shard_name {
            Some(shard_name) => {
                prefix.push_str(shard_name);
                prefix.push_str("/");
                match node_name {
                    Some(node_name) => {
                        prefix.push_str(node_name);
                        prefix.push_str(".json");
                    }
                    None => (),
                };
            }
            None => (),
        };

        let node_metadata = self.node_metadata.read().await;

        // select the node from each shard
        let mut shards: HashMap<String, Vec<String>> = HashMap::new();
        for (key, _metadata) in node_metadata.iter() {
            if key.starts_with(&prefix) {
                let names = match parse_key(&key) {
                    Ok(names) => names,
                    Err(e) => {
                        // ignore keys that do not match the pattern
                        debug!("parse error: error={:?}", e);
                        continue;
                    }
                };

                let shard_key = format!("/{}/{}/", names.0, names.1);
                if shards.contains_key(&shard_key) {
                    let mut node_keys: Vec<String> = shards.get(&shard_key).unwrap().clone();
                    node_keys.push(key.clone());
                    shards.insert(shard_key, node_keys);
                } else {
                    let mut node_keys: Vec<String> = Vec::new();
                    node_keys.push(key.clone());
                    shards.insert(shard_key, node_keys);
                }
            }
        }
        debug!("{:?}", &shards);

        let clients = self.clients.read().await;
        let handles: Vec<JoinHandle<Result<Vec<u8>, Error>>> = shards
            .iter()
            .map(|(shard_key, node_keys)| {
                let key = shard_key.clone();

                let mut primary = String::new();
                let mut replicas: Vec<String> = Vec::new();

                for node_key in node_keys {
                    let m = node_metadata.get(node_key).unwrap();
                    let state = State::from_i32(m.state).unwrap();
                    let role = Role::from_i32(m.role).unwrap();
                    debug!("{:?}", m);
                    if state == State::Ready {
                        match role {
                            Role::Primary => primary = node_key.clone(),
                            Role::Replica => replicas.push(node_key.clone()),
                            Role::Candidate => debug!("{} is {:?}", node_key, &role),
                        }
                    }
                }
                debug!("primary: {}", &primary);
                debug!("replica: {:?}", &replicas);

                let target_key;
                if replicas.len() > 0 {
                    let i = rand::thread_rng().gen_range(0, replicas.len());
                    target_key = replicas.get(i).unwrap();
                } else {
                    target_key = &primary;
                }
                debug!("selected node: {}", target_key);

                let id = id.clone().to_string();

                match clients.get(target_key) {
                    Some(client) => {
                        let client = client.clone();
                        tokio::spawn(async move {
                            let mut client: IndexServiceClient<Channel> = client.into();
                            let req = tonic::Request::new(GetReq { id });
                            match client.get(req).await {
                                Ok(resp) => {
                                    let doc = resp.into_inner().doc;
                                    Ok(doc)
                                }
                                Err(e) => {
                                    match e.code() {
                                        Code::NotFound => Ok(Vec::new()),
                                        _ => Err(Error::from(ErrorKind::Other(format!(
                                            "{}",
                                            e.message()
                                        )))),
                                    }
                                }
                            }
                        })
                    }
                    None => tokio::spawn(async move {
                        Err(Error::from(ErrorKind::Other(format!(
                            "client for {} does not exist", key
                        ))))
                    }),
                }
            })
            .collect::<Vec<_>>();

        let results = match try_join_all(handles).await {
            Ok(results) => results,
            Err(e) => {
                return Err(Error::from(ErrorKind::Other(format!("{}", e.to_string()))));
            }
        };

        let docs = match results.into_iter().collect::<Result<Vec<Vec<u8>>, Error>>() {
            Ok(results) => results,
            Err(e) => {
                return Err(Error::from(ErrorKind::Other(format!("{}", e.to_string()))));
            }
        };

        let mut ret = Vec::new();
        for ((_shard_key, _node_keys), doc) in shards.iter().zip(docs) {
            if doc.len() > 0 {
                ret = doc;
                break;
            }
        }

        Ok(ret)
    }
}
