use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crossbeam::channel::{unbounded, TryRecvError};
use futures::future::try_join_all;
use lazy_static::lazy_static;
use log::*;
use regex::Regex;
use tokio::sync::RwLock;
use tonic::transport::Channel;

use phalanx_common::error::{Error, ErrorKind};
use phalanx_discovery::discovery::nop::TYPE as NOP_TYPE;
use phalanx_discovery::discovery::{DiscoveryContainer, EventType, KeyValuePair};
use phalanx_proto::phalanx::index_service_client::IndexServiceClient;
use phalanx_proto::phalanx::{NodeDetails, ReadinessReq, Role, State};
use tokio::task::JoinHandle;

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
    cache: Arc<RwLock<HashMap<String, Vec<u8>>>>,
    clients: Arc<RwLock<HashMap<String, IndexServiceClient<Channel>>>>,
}

impl Client {
    pub async fn new(discovery_container: DiscoveryContainer) -> Client {
        Client {
            discovery_container,
            watching: Arc::new(AtomicBool::new(false)),
            unwatch: Arc::new(AtomicBool::new(false)),
            cache: Arc::new(RwLock::new(HashMap::new())),
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
        self.cache = Arc::new(RwLock::new(HashMap::new()));
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
                            debug!("{}", e.to_string());
                            continue;
                        }
                    };

                    // update clients
                    if names.2 != "_index_meta" {
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
                        match IndexServiceClient::connect(format!(
                            "http://{}",
                            node_details.address.clone()
                        ))
                        .await
                        {
                            Ok(client) => {
                                debug!("add {} in the client pool", &kvp.key);
                                let mut clients = self.clients.write().await;
                                clients.insert(kvp.key.clone(), client);
                            }
                            Err(e) => {
                                error!(
                                    "failed to connect to {}: {}",
                                    &node_details.address,
                                    e.to_string()
                                );
                            }
                        };
                    }

                    // update cache
                    debug!("add {} in the local cache", &kvp.key);
                    let mut cache = self.cache.write().await;
                    cache.insert(kvp.key.clone(), kvp.value.clone());
                }
            }
            Err(err) => {
                error!("failed to list: error={:?}", err);
            }
        };

        let cache = Arc::clone(&self.cache);
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

                        // update cache
                        match event.event_type {
                            EventType::Put => {
                                debug!("update {} in the local cache", &event.key);
                                let mut cache = cache.write().await;
                                cache.insert(event.key.clone(), event.value.clone());
                            }
                            EventType::Delete => {
                                debug!("delete {} in the local cash", &event.key);
                                let mut cache = cache.write().await;
                                if cache.contains_key(&event.key) {
                                    cache.remove(&event.key);
                                }
                            }
                        }

                        // update clients
                        if names.2 != "_index_meta" {
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
                                    match IndexServiceClient::connect(format!(
                                        "http://{}",
                                        node_details.address.clone()
                                    ))
                                    .await
                                    {
                                        Ok(client) => {
                                            debug!("add {} in the client pool", &event.key);
                                            let mut clients = clients.write().await;
                                            clients.insert(event.key.clone(), client);
                                        }
                                        Err(e) => {
                                            error!(
                                                "failed to connect to {}: {}",
                                                &node_details.address,
                                                e.to_string()
                                            );
                                            let mut clients = clients.write().await;
                                            if clients.contains_key(&event.key) {
                                                clients.remove(&event.key);
                                            }
                                        }
                                    };
                                }
                                EventType::Delete => {
                                    debug!("delete client: key={}", &event.key);
                                    let mut clients = clients.write().await;
                                    if clients.contains_key(&event.key) {
                                        clients.remove(event.key.as_str());
                                    }
                                }
                            };
                        }
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

    pub async fn get_cache(&mut self, key: &str) -> Result<Vec<u8>, Error> {
        let n = self.cache.read().await;
        match n.get(key) {
            Some(value) => Ok(value.clone()),
            None => Err(Error::from(ErrorKind::NotFound(format!(
                "{} does not found",
                key
            )))),
        }
    }

    pub async fn list_cache(&mut self, prefix: &str) -> Result<Vec<KeyValuePair>, Error> {
        let n = self.cache.read().await;

        let mut kvps = Vec::new();
        for (key, value) in n.iter() {
            if key.starts_with(prefix) {
                let kvp = KeyValuePair {
                    key: key.clone(),
                    value: value.clone(),
                };
                kvps.push(kvp);
            }
        }

        Ok(kvps)
    }

    pub async fn get_primary_node(
        &mut self,
        index_name: &str,
        shard_name: &str,
    ) -> Result<NodeDetails, Error> {
        let prefix = format!("/{}/{}/", index_name, shard_name);
        let n = self.cache.read().await;

        for (key, value) in n.iter() {
            if key.starts_with(prefix.as_str()) {
                let node_details = match serde_json::from_slice::<NodeDetails>(value.as_slice()) {
                    Ok(node_details) => node_details,
                    Err(e) => {
                        error!("{}: {}", key, e.to_string());
                        continue;
                    }
                };
                if node_details.state == State::Ready as i32
                    && node_details.role == Role::Primary as i32
                {
                    return Ok(node_details);
                }
            }
        }

        Err(Error::from(ErrorKind::NotFound(format!(
            "primary node does not exist in {}",
            prefix
        ))))
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
            for key in self.cache.read().await.keys() {
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
}
