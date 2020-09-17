use std::collections::HashMap;
use std::error::Error;
use std::io::{Error as IOError, ErrorKind};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::sleep;
use std::time::Duration;

use async_std::task::block_on;
use async_trait::async_trait;
use etcd_client::{Client, EventType, GetOptions, WatchOptions, WatchStream, Watcher};
use log::*;
use regex::Regex;
use tokio::sync::Mutex;

use phalanx_proto::phalanx::index_service_client::IndexServiceClient;
use phalanx_proto::phalanx::{NodeDetails, ReadinessReq, Role, State};

use crate::discovery::{Discovery, NODE_ROLE_GAUGE, NODE_STATE_GAUGE};

pub const TYPE: &str = "etcd";

pub struct Etcd {
    pub client: Client,
    pub root: String,
    pub nodes: Arc<Mutex<HashMap<String, Option<NodeDetails>>>>,
    pub watcher: Option<Arc<Mutex<Watcher>>>,
    pub watch_stream: Option<Arc<Mutex<WatchStream>>>,
    pub stop_health_check_thread: Arc<AtomicBool>,
    pub watch_thread_running: Arc<AtomicBool>,
    pub health_check_thread_running: Arc<AtomicBool>,
}

impl Etcd {
    pub fn new(endpoints: Vec<&str>, root: &str) -> Etcd {
        let conn_future = Client::connect(endpoints, None);

        let client = match block_on(conn_future) {
            Ok(client) => client,
            Err(e) => {
                error!("failed to create etcd client: error = {:?}", e);
                panic!();
            }
        };

        Etcd {
            client,
            root: root.to_string(),
            nodes: Arc::new(Mutex::new(HashMap::new())),
            watcher: None,
            watch_stream: None,
            stop_health_check_thread: Arc::new(AtomicBool::new(false)),
            watch_thread_running: Arc::new(AtomicBool::new(false)),
            health_check_thread_running: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[async_trait]
impl Discovery for Etcd {
    fn get_type(&self) -> &str {
        TYPE
    }

    async fn get_node(
        &mut self,
        index_name: &str,
        shard_name: &str,
        node_name: &str,
    ) -> Result<Option<NodeDetails>, Box<dyn Error + Send + Sync>> {
        let key = format!("{}/{}/{}/{}", &self.root, index_name, shard_name, node_name);
        let get_response = match self.client.get(key.clone(), None).await {
            Ok(get_response) => get_response,
            Err(e) => return Err(Box::new(e)),
        };
        match get_response.kvs().first() {
            Some(kv) => {
                let node_details: NodeDetails =
                    serde_json::from_str(kv.value_str().unwrap()).unwrap();
                Ok(Some(node_details))
            }
            None => Ok(None),
        }
    }

    async fn set_node(
        &mut self,
        index_name: &str,
        shard_name: &str,
        node_name: &str,
        node_details: NodeDetails,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let key = format!("{}/{}/{}/{}", &self.root, index_name, shard_name, node_name);
        let value = serde_json::to_string(&node_details).unwrap();
        match self.client.put(key, value, None).await {
            Ok(_put_response) => Ok(()),
            Err(e) => Err(Box::new(e)),
        }
    }

    async fn delete_node(
        &mut self,
        index_name: &str,
        shard_name: &str,
        node_name: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let key = format!("{}/{}/{}/{}", &self.root, index_name, shard_name, node_name);
        match self.client.delete(key, None).await {
            Ok(_delete_response) => Ok(()),
            Err(e) => Err(Box::new(e)),
        }
    }

    async fn watch(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let watch_thread_running = Arc::clone(&self.watch_thread_running);
        let nodes = Arc::clone(&self.nodes);
        let root = self.root.clone();
        let mut client = self.client.clone();

        if self.watch_thread_running.load(Ordering::Relaxed) {
            warn!("watch is running in another thread");
            return Err(Box::new(IOError::new(
                ErrorKind::Other,
                "watch is running in another thread",
            )));
        }

        match self
            .client
            .watch(self.root.clone(), Some(WatchOptions::new().with_prefix()))
            .await
        {
            Ok((watcher, watch_stream)) => {
                self.watcher = Some(Arc::new(Mutex::new(watcher)));
                self.watch_stream = Some(Arc::new(Mutex::new(watch_stream)));
            }
            Err(e) => {
                error!("failed to etcd watcher: error = {:?}", e);
                return Err(Box::new(IOError::new(
                    ErrorKind::Other,
                    format!("failed to etcd watcher: error = {:?}", e),
                )));
            }
        };

        let watch_stream = match &self.watch_stream {
            Some(watch_stream) => Arc::clone(&watch_stream),
            None => {
                error!("stream is None");
                return Err(Box::new(IOError::new(ErrorKind::Other, "stream is None")));
            }
        };

        tokio::spawn(async move {
            info!("start the watch thread");
            watch_thread_running.store(true, Ordering::Relaxed);

            let re_str = if root.is_empty() {
                "^(/[^/]+/[^/]+)".to_string()
            } else {
                format!("^({}/[^/]+/[^/]+)", &root)
            };
            let re = Regex::new(&re_str).unwrap();

            // initialize a local node cache
            let key = format!("{}/", &root);
            match client
                .get(key.clone(), Some(GetOptions::new().with_prefix()))
                .await
            {
                Ok(get_response) => {
                    let mut nodes = nodes.lock().await;
                    for kv in get_response.kvs() {
                        let key = match kv.key_str() {
                            Ok(key) => key,
                            Err(e) => {
                                error!("failed to get key: error={:?}", e);
                                continue;
                            }
                        };
                        let value = match kv.value_str() {
                            Ok(value) => value,
                            Err(e) => {
                                error!("failed to get value: error={:?}", e);
                                continue;
                            }
                        };

                        let node_details = if value.is_empty() {
                            None
                        } else {
                            match serde_json::from_str(value) {
                                Ok(node_details) => Some(node_details),
                                Err(e) => {
                                    error!("failed to parse JSON: error={:?}", e);
                                    None
                                }
                            }
                        };
                        debug!(
                            "node information has been initialized: key={}, value={:?}",
                            key, &node_details
                        );
                        nodes.insert(key.to_string(), node_details.clone());

                        // update node metrics
                        let tmp_key = &key[&root.len() + 1..];
                        let tmp_key_vec: Vec<&str> = tmp_key.split('/').collect();
                        let index_name = *tmp_key_vec.get(0).unwrap();
                        let shard_name = *tmp_key_vec.get(1).unwrap();
                        let node_name = *tmp_key_vec.get(2).unwrap();
                        match node_details {
                            Some(node_details) => {
                                NODE_STATE_GAUGE
                                    .with_label_values(&[index_name, shard_name, node_name])
                                    .set(node_details.state as f64);
                                NODE_ROLE_GAUGE
                                    .with_label_values(&[index_name, shard_name, node_name])
                                    .set(node_details.role as f64);
                            }
                            None => {}
                        };
                    }
                    debug!("node list has been initialized: nodes={:?}", nodes);
                }
                Err(e) => error!("failed to initialize node list: error={:?}", e),
            };

            loop {
                let mut watch_stream = watch_stream.lock().await;

                let resp = watch_stream.message().await;
                match resp {
                    Ok(resp) => match resp {
                        Some(resp) => {
                            // receive events watching has cancelled
                            if resp.canceled() {
                                debug!("a request to stop the watch thread has been received");
                                break;
                            }

                            // receive events
                            for event in resp.events() {
                                if let Some(kv) = event.kv() {
                                    let key = match kv.key_str() {
                                        Ok(key) => key,
                                        Err(e) => {
                                            error!("failed to get key: error={:?}", e);
                                            continue;
                                        }
                                    };
                                    let value = match kv.value_str() {
                                        Ok(value) => value,
                                        Err(e) => {
                                            error!("failed to get value: error={:?}", e);
                                            continue;
                                        }
                                    };

                                    // parse JSON
                                    let node_details = if value.is_empty() {
                                        None
                                    } else {
                                        match serde_json::from_str(value) {
                                            Ok(node_details) => Some(node_details),
                                            Err(e) => {
                                                error!("failed to parse JSON: error={:?}", e);
                                                None
                                            }
                                        }
                                    };

                                    // update local node cache
                                    let mut nodes = nodes.lock().await;
                                    match event.event_type() {
                                        EventType::Put => {
                                            debug!("node information has been updated: key={}, value={:?}", key, &node_details);
                                            nodes.insert(key.to_string(), node_details.clone());

                                            // update node metrics
                                            let tmp_key = &key[&root.len() + 1..];
                                            let tmp_key_vec: Vec<&str> =
                                                tmp_key.split('/').collect();
                                            let index_name = *tmp_key_vec.get(0).unwrap();
                                            let shard_name = *tmp_key_vec.get(1).unwrap();
                                            let node_name = *tmp_key_vec.get(2).unwrap();
                                            match node_details {
                                                Some(node_details) => {
                                                    NODE_STATE_GAUGE
                                                        .with_label_values(&[
                                                            index_name, shard_name, node_name,
                                                        ])
                                                        .set(node_details.state as f64);
                                                    NODE_ROLE_GAUGE
                                                        .with_label_values(&[
                                                            index_name, shard_name, node_name,
                                                        ])
                                                        .set(node_details.role as f64);
                                                }
                                                None => {}
                                            };
                                        }
                                        EventType::Delete => {
                                            debug!(
                                                "node information has been deleted: key={}",
                                                key
                                            );
                                            nodes.remove(key);
                                        }
                                    };

                                    debug!("node list has changed: nodes={:?}", nodes);

                                    // get the shard where the event occurred
                                    let shard = match re.captures(key) {
                                        Some(cap) => match cap.get(1) {
                                            Some(m) => m.as_str(),
                                            None => {
                                                debug!("key doesn't match: key={}", key);
                                                ""
                                            }
                                        },
                                        None => {
                                            debug!("key doesn't match: key={}", key);
                                            ""
                                        }
                                    };

                                    // make a ready candidate a replica
                                    match client
                                        .get(
                                            shard.to_string(),
                                            Some(GetOptions::new().with_prefix()),
                                        )
                                        .await
                                    {
                                        Ok(get_response) => {
                                            for kv in get_response.kvs() {
                                                let key = match kv.key_str() {
                                                    Ok(key) => key,
                                                    Err(e) => {
                                                        error!("failed to get key: error={:?}", e);
                                                        continue;
                                                    }
                                                };
                                                let value = match kv.value_str() {
                                                    Ok(value) => value,
                                                    Err(e) => {
                                                        error!(
                                                            "failed to get value: error={:?}",
                                                            e
                                                        );
                                                        continue;
                                                    }
                                                };

                                                if !value.is_empty() {
                                                    match serde_json::from_str::<NodeDetails>(value)
                                                    {
                                                        Ok(mut node_details) => {
                                                            if node_details.state
                                                                == State::Ready as i32
                                                                && node_details.role
                                                                    == Role::Candidate as i32
                                                            {
                                                                node_details.role =
                                                                    Role::Replica as i32;
                                                                let new_value =
                                                                    serde_json::to_string(
                                                                        &node_details,
                                                                    )
                                                                    .unwrap();
                                                                match client.put(key.to_string(), new_value, None).await {
                                                                    Ok(_put_response) => debug!("node details has been successfully updated"),
                                                                    Err(e) => error!("failed to update node details: error={:?}", e),
                                                                };
                                                            }
                                                        }
                                                        Err(e) => {
                                                            error!(
                                                                "failed to parse JSON: error={:?}",
                                                                e
                                                            );
                                                        }
                                                    };
                                                }
                                            }
                                        }
                                        Err(e) => error!(
                                            "failed to get a ready candidate list: error={:?}",
                                            e
                                        ),
                                    };

                                    // make a ready replica a primary
                                    match client
                                        .get(
                                            shard.to_string(),
                                            Some(GetOptions::new().with_prefix()),
                                        )
                                        .await
                                    {
                                        Ok(get_response) => {
                                            let mut primary_exists = false;

                                            // check whether the shard group has a primary node
                                            for kv in get_response.kvs() {
                                                let _key = match kv.key_str() {
                                                    Ok(key) => key,
                                                    Err(e) => {
                                                        error!("failed to get key: error={:?}", e);
                                                        continue;
                                                    }
                                                };
                                                let value = match kv.value_str() {
                                                    Ok(value) => value,
                                                    Err(e) => {
                                                        error!(
                                                            "failed to get value: error={:?}",
                                                            e
                                                        );
                                                        continue;
                                                    }
                                                };

                                                if !value.is_empty() {
                                                    match serde_json::from_str::<NodeDetails>(value)
                                                    {
                                                        Ok(node_details) => {
                                                            if node_details.state
                                                                == State::Ready as i32
                                                                && node_details.role
                                                                    == Role::Primary as i32
                                                            {
                                                                primary_exists = true;
                                                            }
                                                        }
                                                        Err(e) => {
                                                            error!(
                                                                "failed to parse JSON: error={:?}",
                                                                e
                                                            );
                                                        }
                                                    };
                                                }
                                            }

                                            if !primary_exists {
                                                // make one of the replica nodes primary
                                                for kv in get_response.kvs() {
                                                    let key = match kv.key_str() {
                                                        Ok(key) => key,
                                                        Err(e) => {
                                                            error!(
                                                                "failed to get key: error={:?}",
                                                                e
                                                            );
                                                            continue;
                                                        }
                                                    };
                                                    let value = match kv.value_str() {
                                                        Ok(value) => value,
                                                        Err(e) => {
                                                            error!(
                                                                "failed to get value: error={:?}",
                                                                e
                                                            );
                                                            continue;
                                                        }
                                                    };

                                                    if !value.is_empty() {
                                                        match serde_json::from_str::<NodeDetails>(
                                                            value,
                                                        ) {
                                                            Ok(mut node_details) => {
                                                                if node_details.state
                                                                    == State::Ready as i32
                                                                    && node_details.role
                                                                        == Role::Replica as i32
                                                                {
                                                                    node_details.role =
                                                                        Role::Primary as i32;
                                                                    let new_value =
                                                                        serde_json::to_string(
                                                                            &node_details,
                                                                        )
                                                                        .unwrap();
                                                                    match client.put(key.to_string(), new_value, None).await {
                                                                        Ok(_put_response) => debug!("node details has been successfully updated"),
                                                                        Err(e) => error!("failed to update node details: error={:?}", e),
                                                                    };
                                                                }
                                                            }
                                                            Err(e) => {
                                                                error!("failed to parse JSON: error={:?}", e);
                                                            }
                                                        };
                                                    }
                                                }
                                            }
                                        }
                                        Err(e) => error!(
                                            "failed to get a ready replica list: error={:?}",
                                            e
                                        ),
                                    };
                                }
                            }
                        }
                        None => {
                            warn!("watch response is None");
                        }
                    },
                    Err(e) => {
                        error!("failed to get watch response: error={:?}", e);
                    }
                };
            }

            watch_thread_running.store(false, Ordering::Relaxed);
            info!("exit the watch thread");
        });

        Ok(())
    }

    async fn unwatch(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        if !self.watch_thread_running.load(Ordering::Relaxed) {
            warn!("watch thread is not running");
            return Err(Box::new(IOError::new(
                ErrorKind::Other,
                "watch thread is not running",
            )));
        }

        let watcher = match &self.watcher {
            Some(watcher) => Arc::clone(&watcher),
            None => {
                error!("stream is None");
                return Err(Box::new(IOError::new(ErrorKind::Other, "stream is None")));
            }
        };

        let mut watcher = watcher.lock().await;

        match watcher.cancel().await {
            Ok(_) => Ok(()),
            Err(e) => Err(Box::new(e)),
        }
    }

    async fn start_health_check(
        &mut self,
        interval: u64,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let health_check_thread_running = Arc::clone(&self.health_check_thread_running);
        let stop_health_check_thread = Arc::clone(&self.stop_health_check_thread);
        let nodes = Arc::clone(&self.nodes);
        let mut client = self.client.clone();

        if self.health_check_thread_running.load(Ordering::Relaxed) {
            warn!("health check is running in another thread");
            return Err(Box::new(IOError::new(
                ErrorKind::Other,
                "health check is running in another thread",
            )));
        }

        tokio::spawn(async move {
            info!("start a health check thread");
            health_check_thread_running.store(true, Ordering::Relaxed);

            loop {
                if stop_health_check_thread.load(Ordering::Relaxed) {
                    debug!("a request to stop the health check thread has been received");

                    // restore stop flag to false
                    stop_health_check_thread.store(false, Ordering::Relaxed);

                    break;
                } else {
                    let nodes = nodes.lock().await;

                    for (key, node_details) in nodes.iter() {
                        // health check
                        match node_details {
                            Some(node_details) => {
                                let grpc_server_url = format!("http://{}", node_details.address);
                                match IndexServiceClient::connect(grpc_server_url.clone()).await {
                                    Ok(mut grpc_client) => {
                                        let readiness_req = tonic::Request::new(ReadinessReq {});
                                        match grpc_client.readiness(readiness_req).await {
                                            Ok(resp) => match resp.into_inner().state {
                                                state if state == State::Ready as i32 => {
                                                    // node is ready
                                                    if node_details.state != State::Ready as i32 {
                                                        let new_node_details = NodeDetails {
                                                            address: node_details.address.clone(),
                                                            state: State::Ready as i32,
                                                            role: node_details.role,
                                                        };
                                                        let value = serde_json::to_string(
                                                            &new_node_details,
                                                        )
                                                        .unwrap();
                                                        debug!(
                                                            "node is ready: key={}, value={}",
                                                            key, &value
                                                        );
                                                        match client.put(key.to_string(), value, None).await {
                                                            Ok(_put_response) => debug!("node details has been successfully updated"),
                                                            Err(e) => error!("failed to update node details: error={:?}", e),
                                                        };
                                                    }
                                                }
                                                _ => {
                                                    // node is not ready
                                                    if node_details.state != State::NotReady as i32
                                                    {
                                                        let new_node_details = NodeDetails {
                                                            address: node_details.address.clone(),
                                                            state: State::NotReady as i32,
                                                            role: Role::Candidate as i32,
                                                        };
                                                        let value = serde_json::to_string(
                                                            &new_node_details,
                                                        )
                                                        .unwrap();
                                                        warn!(
                                                            "node is not ready: key={}, value={}",
                                                            key, &value
                                                        );
                                                        match client.put(key.to_string(), value, None).await {
                                                            Ok(_put_response) => debug!("node details has been successfully updated"),
                                                            Err(e) => error!("failed to update node details: error={:?}", e),
                                                        };
                                                    }
                                                }
                                            },
                                            Err(e) => {
                                                // request failed
                                                if node_details.state != State::NotReady as i32 {
                                                    let new_node_details = NodeDetails {
                                                        address: node_details.address.clone(),
                                                        state: State::NotReady as i32,
                                                        role: Role::Candidate as i32,
                                                    };
                                                    let value =
                                                        serde_json::to_string(&new_node_details)
                                                            .unwrap();
                                                    error!("node health check failed: key={}, value={}, error={:?}", key, &value, e);
                                                    match client.put(key.to_string(), value, None).await {
                                                        Ok(_put_response) => debug!("node details has been successfully updated"),
                                                        Err(e) => error!("failed to update node details: error={:?}", e),
                                                    };
                                                }
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        // connection failed
                                        if node_details.state != State::Disconnected as i32 {
                                            let new_node_details = NodeDetails {
                                                address: node_details.address.clone(),
                                                state: State::Disconnected as i32,
                                                role: Role::Candidate as i32,
                                            };
                                            let value =
                                                serde_json::to_string(&new_node_details).unwrap();
                                            error!(
                                                "connection to the node has been disconnected: key={}, value={}, error={:?}",
                                                key, &value, e
                                            );
                                            match client.put(key.to_string(), value, None).await {
                                                Ok(_put_response) => debug!(
                                                    "node details has been successfully updated"
                                                ),
                                                Err(e) => error!(
                                                    "failed to update node details: error={:?}",
                                                    e
                                                ),
                                            };
                                        }
                                    }
                                }
                            }
                            None => {
                                // node details does not exist
                                debug!("node details does not exist: key={}", key);
                                match client.delete(key.to_string(), None).await {
                                    Ok(_delete_response) => {
                                        debug!("node details has been successfully deleted")
                                    }
                                    Err(e) => {
                                        error!("failed to delete node details: error={:?}", e)
                                    }
                                }
                            }
                        };
                    }
                }

                sleep(Duration::from_millis(interval));
            }

            health_check_thread_running.store(false, Ordering::Relaxed);
            info!("exit the health check thread");
        });

        Ok(())
    }

    async fn stop_health_check(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        if !self.health_check_thread_running.load(Ordering::Relaxed) {
            warn!("health check thread is not running");
            return Err(Box::new(IOError::new(
                ErrorKind::Other,
                "health check thread is not running",
            )));
        }

        self.stop_health_check_thread.store(true, Ordering::Relaxed);

        Ok(())
    }
}
