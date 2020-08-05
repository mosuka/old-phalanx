use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

use crossbeam_channel::{unbounded, Sender, TryRecvError};
use log::*;

use phalanx_proto::phalanx::index_service_client::IndexServiceClient;
use phalanx_proto::phalanx::{ReadinessReq, Role, State};

use crate::overseer::{Message, Worker, WorkerError};
use phalanx_discovery::discovery::Discovery;

pub struct Overseer {
    sender: Option<Sender<Message>>,
    handle: Option<JoinHandle<()>>,
    discovery: Arc<Mutex<Box<dyn Discovery>>>,
    period: u64,
}

impl Overseer {
    pub fn new(discovery: Box<dyn Discovery>, period: u64) -> Self {
        Self {
            sender: None,
            handle: None,
            discovery: Arc::new(Mutex::new(discovery)),
            period,
        }
    }

    pub fn update_status(&self, url: &str) {
        info!("check: {}", url);
    }
}

impl Worker for Overseer {
    type Error = WorkerError;

    fn run(&mut self) {
        let (tx, rx) = unbounded();
        let discovery = self.discovery.clone();
        let period = self.period.clone();
        let handle = thread::spawn(move || {
            let mut d = discovery.lock().unwrap();

            let mut rt = tokio::runtime::Runtime::new().unwrap();
            loop {
                match rx.try_recv() {
                    Ok(Message::Stop) | Err(TryRecvError::Disconnected) => {
                        // stop the thread when a stop message received or the channel is closed (=Worker is destroyed)
                        break;
                    }
                    Err(TryRecvError::Empty) => {
                        rt.block_on( async {
                            for index_name in d.get_indices().await.unwrap() {
                                for shard_name in d.get_shards(&index_name).await.unwrap() {
                                    for (node_name, node_details) in d.get_nodes(&index_name, &shard_name).await.unwrap() {
                                        match node_details {
                                            Some(mut node_details) => {
                                                debug!("health check: index_name={}, shard_name={}, node_name={} node_status={:?}", &index_name, &shard_name, &node_name, &node_details);
                                                let grpc_server_url = format!("http://{}", &node_details.address);
                                                match IndexServiceClient::connect(grpc_server_url.clone()).await {
                                                    Ok(mut grpc_client) => {
                                                        // health check
                                                        let readiness_req = tonic::Request::new(ReadinessReq {});
                                                        match grpc_client.readiness(readiness_req).await {
                                                            Ok(resp) => {
                                                                match resp.into_inner().state {
                                                                    state if state == State::Ready as i32 => {
                                                                        // ready
                                                                        if node_details.state != State::Ready as i32 {
                                                                            node_details.state = State::Ready as i32;
                                                                            d.set_node(&index_name, &shard_name, &node_name, node_details.clone()).await.unwrap();
                                                                            debug!("node state has changed: index_name={}, shard_name={}, node_name={} node_status={:?}", &index_name, &shard_name, &node_name, &node_details);
                                                                        }
                                                                    }
                                                                    _ => {
                                                                        // not ready
                                                                        if node_details.state != State::NotReady as i32 {
                                                                            node_details.role = Role::Candidate as i32;
                                                                            node_details.state = State::NotReady as i32;
                                                                            d.set_node(&index_name, &shard_name, &node_name, node_details.clone()).await.unwrap();
                                                                            debug!("node state has changed: index_name={}, shard_name={}, node_name={} node_status={:?}", &index_name, &shard_name, &node_name, &node_details);
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                            Err(e) => {
                                                                error!("failed to request to server: error={:?}", e);

                                                                // failed to request to server
                                                                if node_details.state != State::NotReady as i32 {
                                                                    node_details.role = Role::Candidate as i32;
                                                                    node_details.state = State::NotReady as i32;
                                                                    d.set_node(&index_name, &shard_name, &node_name, node_details.clone()).await.unwrap();
                                                                    debug!("node state has changed: index_name={}, shard_name={}, node_name={} node_status={:?}", &index_name, &shard_name, &node_name, &node_details);
                                                                }
                                                            }
                                                        }
                                                    }
                                                    Err(e) => {
                                                        error!("failed to request to server: error={:?}", e);

                                                        // failed to connect to server
                                                        if node_details.state != State::Disconnected as i32 {
                                                            node_details.role = Role::Candidate as i32;
                                                            node_details.state = State::Disconnected as i32;
                                                            d.set_node(&index_name, &shard_name, &node_name, node_details.clone()).await.unwrap();
                                                            debug!("node state has changed: index_name={}, shard_name={}, node_name={} node_status={:?}", &index_name, &shard_name, &node_name, &node_details);
                                                        }
                                                    }
                                                }
                                            }
                                            None => {
                                                // key does not exist
                                                debug!("node status does not exist: index_name={}, shard_name={}, node_name={}", &index_name, &shard_name, &node_name);
                                                d.delete_node(&index_name, &shard_name, &node_name).await.unwrap();
                                            }
                                        };
                                    }

                                    // get primary nodes
                                    match d.get_primary_node(&index_name, &shard_name).await {
                                        Ok(node_name_opt) => {
                                            match node_name_opt {
                                                Some(_node_name) => {
                                                    // change candidate nodes to replica nodes if primary node exists
                                                    match d.get_candidate_nodes(&index_name, &shard_name).await {
                                                        Ok(candidates) => {
                                                            for (node_name, node_details) in candidates {
                                                                match node_details {
                                                                    Some(mut node_details) => {
                                                                        node_details.role = Role::Replica as i32;
                                                                        match d.set_node(&index_name, &shard_name, &node_name, node_details.clone()).await {
                                                                            Ok(_) => {
                                                                                info!("node has become a replica node: index_name={}, shard_name={}, node_name={}, node_status={:?}", &index_name, &shard_name, &node_name, &node_details);
                                                                            },
                                                                            Err(e) => {
                                                                                error!("failed to set node status: index_name={}, shard_name={}, node_name={}, node_status={:?}, error={:?}", &index_name, &shard_name, &node_name, &node_details, e);
                                                                            },
                                                                        };
                                                                    }
                                                                    None => {
                                                                        // key does not exist
                                                                        debug!("node status does not exist: index_name={}, shard_name={}, node_name={}", index_name, shard_name, &node_name);
                                                                        d.delete_node(&index_name, &shard_name, &node_name).await.unwrap();
                                                                    }
                                                                }

                                                            }
                                                        }
                                                        Err(e) => {
                                                            error!("failed to get candidates: index_name={}, shard_name={}, error={:?}", &index_name, &shard_name, e);
                                                        },
                                                    };
                                                },
                                                None => {
                                                    // change candidate nodes to replica nodes
                                                    match d.get_candidate_nodes(&index_name, &shard_name).await {
                                                        Ok(candidates) => {
                                                            for (node_name, node_details) in candidates {
                                                                match node_details {
                                                                    Some(mut node_details) => {
                                                                        node_details.role = Role::Replica as i32;
                                                                        match d.set_node(&index_name, &shard_name, &node_name, node_details.clone()).await {
                                                                            Ok(_) => {
                                                                                info!("node has become a replica node: index_name={}, shard_name={}, node_name={}, node_status={:?}", &index_name, &shard_name, &node_name, &node_details);
                                                                            },
                                                                            Err(e) => {
                                                                                error!("failed to set node status: index_name={}, shard_name={}, node_name={}, node_status={:?}, error={:?}", &index_name, &shard_name, &node_name, &node_details, e);
                                                                            },
                                                                        };
                                                                    }
                                                                    None => {
                                                                        // key does not exist
                                                                        debug!("node status does not exist: index_name={}, shard_name={}, node_name={}", index_name, shard_name, &node_name);
                                                                        d.delete_node(&index_name, &shard_name, &node_name).await.unwrap();
                                                                    }
                                                                }

                                                            }
                                                        }
                                                        Err(e) => {
                                                            error!("failed to get candidates: index_name={}, shard_name={}, error={:?}", &index_name, &shard_name, e);
                                                        },
                                                    };

                                                    // change one of replica nodes to primary node if primary node does not exist
                                                    match d.get_replica_nodes(&index_name, &shard_name).await {
                                                        Ok(replicas) => {
                                                            for (node_name, node_details) in replicas {
                                                                match node_details {
                                                                    Some(mut node_details) => {
                                                                        node_details.role = Role::Primary as i32;
                                                                        match d.set_node(&index_name, &shard_name, &node_name, node_details.clone()).await {
                                                                            Ok(_) => {
                                                                                info!("node has become a primary node: index_name={}, shard_name={}, node_name={}, node_status={:?}", &index_name, &shard_name, &node_name, &node_details);
                                                                                break;
                                                                            },
                                                                            Err(e) => {
                                                                                error!("failed to set node status: index_name={}, shard_name={}, node_name={}, node_status={:?}, error={:?}", &index_name, &shard_name, &node_name, &node_details, e);
                                                                            },
                                                                        };
                                                                    }
                                                                    None => {
                                                                        // key does not exist
                                                                        debug!("node status does not exist: index_name={}, shard_name={}, node_name={}", index_name, shard_name, &node_name);
                                                                        d.delete_node(&index_name, &shard_name, &node_name).await.unwrap();
                                                                    }
                                                                }
                                                            }
                                                        },
                                                        Err(e) => {
                                                            error!("failed to get replica nodes: index_name={}, shard_name={}, error={:?}", &index_name, &shard_name, e);
                                                        },
                                                    };
                                                },
                                            }
                                        }
                                        Err(e) => {
                                            error!("failed to get primary node: index_name={}, shard_name={}, error={:?}", &index_name, &shard_name, e);
                                        },
                                    }
                                }
                            }
                        });

                        thread::sleep(Duration::from_millis(period));
                    }
                }
            }
        });

        self.sender = Some(tx);
        self.handle = Some(handle);
    }

    fn stop(&mut self) -> Result<(), Self::Error> {
        if let (Some(sender), Some(handle)) = (self.sender.take(), self.handle.take()) {
            // send a stop message to a thread
            sender.send(Message::Stop).map_err(WorkerError::Channel)?;
            // wait for the end
            handle.join().map_err(WorkerError::Thread)?;
            Ok(())
        } else {
            Err(WorkerError::ThreadNotStarted)
        }
    }
}
