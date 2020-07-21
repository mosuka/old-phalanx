use std::any::Any;
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

use crossbeam_channel::{unbounded, SendError, Sender, TryRecvError};
use log::*;

use phalanx_discovery::discovery::{Discovery, State as NodeState};
use phalanx_proto::index::index_service_client::IndexServiceClient;
use phalanx_proto::index::{HealthReq, State as HealthState};

pub enum Message {
    Stop,
}

#[derive(Debug)]
pub enum WorkerError {
    Channel(SendError<Message>),
    Thread(Box<dyn Any + Send + 'static>),
    ThreadNotStarted,
}

pub trait Worker {
    type Error;
    fn run(&mut self);
    fn stop(&mut self) -> Result<(), Self::Error>;
}

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
                        let task = async {
                            let nodes = d.get_nodes().await.unwrap();
                            for (node_key, mut node_status) in nodes {
                                debug!("key={:?}, node_status={:?}", &node_key, &node_status);
                                let grpc_server_url = format!("http://{}", &node_status.address);
                                match IndexServiceClient::connect(grpc_server_url.clone()).await {
                                    Ok(mut grpc_client) => {
                                        // health check
                                        let req = tonic::Request::new(HealthReq {});
                                        match grpc_client.health(req).await {
                                            Ok(resp) => {
                                                match resp.into_inner().state {
                                                    state if state == HealthState::Ready as i32 => {
                                                        // Ready
                                                        // change node status to active
                                                        if node_status.state != NodeState::Active {
                                                            node_status.state = NodeState::Active;
                                                            d.set_node(
                                                                node_key.clone(),
                                                                node_status.clone(),
                                                            )
                                                            .await
                                                            .unwrap();
                                                            info!("node state has changed: node_key={:?}, node_status={:?}", &node_key, &node_status);
                                                        }
                                                    }
                                                    state
                                                        if state
                                                            == HealthState::NotReady as i32 =>
                                                    {
                                                        // NotReady
                                                        // change node status to inactive
                                                        if node_status.state != NodeState::Inactive
                                                        {
                                                            node_status.state = NodeState::Inactive;
                                                            d.set_node(
                                                                node_key.clone(),
                                                                node_status.clone(),
                                                            )
                                                            .await
                                                            .unwrap();
                                                            warn!("node state has changed: node_key={:?}, node_status={:?}", &node_key, &node_status);
                                                        }
                                                    }
                                                    _ => {
                                                        // Unknown
                                                        // change node status to inactive
                                                        if node_status.state != NodeState::Inactive
                                                        {
                                                            node_status.state = NodeState::Inactive;
                                                            d.set_node(
                                                                node_key.clone(),
                                                                node_status.clone(),
                                                            )
                                                            .await
                                                            .unwrap();
                                                            error!("node state has changed: node_key={:?}, node_status={:?}", &node_key, &node_status);
                                                        }
                                                    }
                                                };
                                            }
                                            Err(e) => {
                                                error!(
                                                    "failed to check node health {}: error={}",
                                                    &grpc_server_url, e
                                                );

                                                // change node status to inactive
                                                if node_status.state != NodeState::Inactive {
                                                    node_status.state = NodeState::Inactive;
                                                    d.set_node(
                                                        node_key.clone(),
                                                        node_status.clone(),
                                                    )
                                                    .await
                                                    .unwrap();
                                                    warn!("node state has changed: node_key={:?}, node_status={:?}", &node_key, &node_status);
                                                }
                                            }
                                        };
                                    }
                                    Err(e) => {
                                        error!(
                                            "failed to connect {}: error={}",
                                            &grpc_server_url, e
                                        );

                                        // change node status to inactive
                                        if node_status.state != NodeState::Inactive {
                                            node_status.state = NodeState::Inactive;
                                            d.set_node(node_key.clone(), node_status.clone())
                                                .await
                                                .unwrap();
                                            warn!("node state has changed: node_key={:?}, node_status={:?}", &node_key, &node_status);
                                        }
                                    }
                                }
                            }
                        };

                        rt.block_on(task);
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
