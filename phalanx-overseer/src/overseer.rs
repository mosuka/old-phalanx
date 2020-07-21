use std::any::Any;
use std::sync::mpsc::{channel, SendError, Sender, TryRecvError};
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;
use std::error::Error;

use async_std::task::block_on;
use log::*;

use phalanx_discovery::discovery::Discovery;
use phalanx_proto::index::index_service_client::IndexServiceClient;
use phalanx_proto::index::{HealthReq, State};

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
        let (tx, rx) = channel();
        let discovery = self.discovery.clone();
        let period = self.period.clone();
        let handle = thread::spawn(move || {
            let mut d = discovery.lock().unwrap();
            loop {
                match rx.try_recv() {
                    Ok(Message::Stop) | Err(TryRecvError::Disconnected) => {
                        // stop the thread when a stop message received or the channel is closed (=Worker is destroyed)
                        break;
                    }
                    Err(TryRecvError::Empty) => {
                        let future = d.get_nodes();
                        match block_on(future) {
                            Ok(nodes) => {
                                for (k, node_status) in nodes {
                                    info!("key={}, node_status={:?}", k, node_status);

                                    let grpc_server_url = format!("http://{}", node_status.address.clone());
                                    let grpc_client_future = IndexServiceClient::connect(grpc_server_url);
                                    // match block_on(grpc_client_future) {
                                    //     Ok(mut grpc_client) => {
                                    //         let req = tonic::Request::new(HealthReq {});
                                    //
                                    //         let health_future = grpc_client.health(req);
                                    //         match block_on(health_future) {
                                    //             Ok(resp) => {
                                    //                 let state = resp.into_inner().state;
                                    //                 match state {
                                    //                     state if state == State::Ready as i32 => {
                                    //                         info!("ready");
                                    //                     }
                                    //                     state if state == State::NotReady as i32 => {
                                    //                         info!("not ready");
                                    //                     }
                                    //                     _ => {
                                    //                         info!("not ready");
                                    //                     }
                                    //                 };
                                    //             },
                                    //             Err(e) => error!("failed to connect: error={}", e),
                                    //         };
                                    //     },
                                    //     Err(e) => error!("failed to create gRPC client: error={}", e),
                                    // };
                                }
                            },
                            Err(e) => error!("failed to get nodes: error={}", e),
                        };
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
