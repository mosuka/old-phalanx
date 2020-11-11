use std::sync::Arc;

use lazy_static::lazy_static;
use prometheus::{register_counter_vec, register_histogram_vec, CounterVec, HistogramVec};
use tokio::sync::Mutex;
use tonic::{Code, Request, Response, Status};

use phalanx_proto::phalanx::overseer_service_server::OverseerService as ProtoOverseerService;
use phalanx_proto::phalanx::{
    InquireReply, InquireReq, ReadinessReply, ReadinessReq, RegisterReply, RegisterReq, State,
    UnregisterReply, UnregisterReq, UnwatchReply, UnwatchReq, WatchReply, WatchReq,
};

use crate::overseer::Overseer;

lazy_static! {
    static ref REQUEST_COUNTER: CounterVec = register_counter_vec!(
        "phalanx_overseer_requests_total",
        "Total number of requests.",
        &["func"]
    )
    .unwrap();
    static ref REQUEST_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "phalanx_overseer_request_duration_seconds",
        "The request latencies in seconds.",
        &["func"]
    )
    .unwrap();
}

pub struct OverseerService {
    overseer: Arc<Mutex<Overseer>>,
}

impl OverseerService {
    pub fn new(overseer: Overseer) -> OverseerService {
        OverseerService {
            overseer: Arc::new(Mutex::new(overseer)),
        }
    }
}

#[tonic::async_trait]
impl ProtoOverseerService for OverseerService {
    async fn readiness(
        &self,
        _request: Request<ReadinessReq>,
    ) -> Result<Response<ReadinessReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["readiness"]).inc();
        let timer = REQUEST_HISTOGRAM
            .with_label_values(&["readiness"])
            .start_timer();

        let state = State::Ready as i32;

        let reply = ReadinessReply { state };

        timer.observe_duration();

        Ok(Response::new(reply))
    }

    async fn watch(&self, request: Request<WatchReq>) -> Result<Response<WatchReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["watch"]).inc();
        let timer = REQUEST_HISTOGRAM
            .with_label_values(&["watch"])
            .start_timer();

        let req = request.into_inner();

        let interval = req.interval;

        let overseer = Arc::clone(&self.overseer);
        let mut overseer = overseer.lock().await;

        match overseer.watch().await {
            Ok(_) => (),
            Err(e) => {
                timer.observe_duration();

                return Err(Status::new(
                    Code::Internal,
                    format!("failed to watch: error = {:?}", e),
                ));
            }
        };

        match overseer.receive().await {
            Ok(_) => (),
            Err(e) => {
                timer.observe_duration();

                return Err(Status::new(
                    Code::Internal,
                    format!("failed to receive: error = {:?}", e),
                ));
            }
        };

        match overseer.probe(interval).await {
            Ok(_) => (),
            Err(e) => {
                timer.observe_duration();

                return Err(Status::new(
                    Code::Internal,
                    format!("failed to probe: error = {:?}", e),
                ));
            }
        };

        let reply = WatchReply {};

        timer.observe_duration();

        Ok(Response::new(reply))
    }

    async fn unwatch(
        &self,
        _request: Request<UnwatchReq>,
    ) -> Result<Response<UnwatchReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["unwatch"]).inc();
        let timer = REQUEST_HISTOGRAM
            .with_label_values(&["unwatch"])
            .start_timer();

        let overseer = Arc::clone(&self.overseer);
        let mut overseer = overseer.lock().await;

        match overseer.unwatch().await {
            Ok(_) => (),
            Err(e) => {
                timer.observe_duration();

                return Err(Status::new(
                    Code::Internal,
                    format!("failed to unwatch: error = {:?}", e),
                ));
            }
        };

        match overseer.unreceive().await {
            Ok(_) => (),
            Err(e) => {
                timer.observe_duration();

                return Err(Status::new(
                    Code::Internal,
                    format!("failed to unreceive: error = {:?}", e),
                ));
            }
        };

        match overseer.unprobe().await {
            Ok(_) => (),
            Err(e) => {
                timer.observe_duration();

                return Err(Status::new(
                    Code::Internal,
                    format!("failed to unprobe: error = {:?}", e),
                ));
            }
        }

        let reply = UnwatchReply {};

        timer.observe_duration();

        Ok(Response::new(reply))
    }

    async fn register(
        &self,
        request: Request<RegisterReq>,
    ) -> Result<Response<RegisterReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["register"]).inc();
        let timer = REQUEST_HISTOGRAM
            .with_label_values(&["register"])
            .start_timer();

        let req = request.into_inner();

        let index_name = &req.index_name;
        let shard_name = &req.shard_name;
        let node_name = &req.node_name;

        let node_details = req.node_details.unwrap();

        let overseer = Arc::clone(&self.overseer);
        let mut overseer = overseer.lock().await;

        match overseer
            .register(index_name, shard_name, node_name, node_details)
            .await
        {
            Ok(_) => {
                let reply = RegisterReply {};

                timer.observe_duration();

                Ok(Response::new(reply))
            }
            Err(e) => {
                timer.observe_duration();

                Err(Status::new(
                    Code::Internal,
                    format!("failed to register node: error = {:?}", e),
                ))
            }
        }
    }

    async fn unregister(
        &self,
        request: Request<UnregisterReq>,
    ) -> Result<Response<UnregisterReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["unregister"]).inc();
        let timer = REQUEST_HISTOGRAM
            .with_label_values(&["unregister"])
            .start_timer();

        let req = request.into_inner();

        let index_name = &req.index_name;
        let shard_name = &req.shard_name;
        let node_name = &req.node_name;

        let overseer = Arc::clone(&self.overseer);
        let mut overseer = overseer.lock().await;

        match overseer.unregister(index_name, shard_name, node_name).await {
            Ok(_) => {
                let reply = UnregisterReply {};

                timer.observe_duration();

                Ok(Response::new(reply))
            }
            Err(e) => {
                timer.observe_duration();

                Err(Status::new(
                    Code::Internal,
                    format!("failed to unregister node: error = {:?}", e),
                ))
            }
        }
    }

    async fn inquire(
        &self,
        request: Request<InquireReq>,
    ) -> Result<Response<InquireReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["inquire"]).inc();
        let timer = REQUEST_HISTOGRAM
            .with_label_values(&["inquire"])
            .start_timer();

        let req = request.into_inner();

        let index_name = &req.index_name;
        let shard_name = &req.shard_name;
        let node_name = &req.node_name;

        let overseer = Arc::clone(&self.overseer);
        let mut overseer = overseer.lock().await;

        match overseer.inquire(index_name, shard_name, node_name).await {
            Ok(node_details) => {
                let reply = InquireReply { node_details };

                timer.observe_duration();

                Ok(Response::new(reply))
            }
            Err(e) => {
                timer.observe_duration();

                Err(Status::new(
                    Code::Internal,
                    format!("failed to inquire node: error = {:?}", e),
                ))
            }
        }
    }
}
