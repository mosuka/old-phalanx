use std::sync::Arc;

use log::*;
use prometheus::{CounterVec, HistogramVec};
use tokio::sync::Mutex;
use tonic::{Code, Request, Response, Status};

use phalanx_discovery::discovery::Discovery;
use phalanx_proto::phalanx::overseer_service_server::OverseerService as ProtoOverseerService;
use phalanx_proto::phalanx::{
    InquireReply, InquireReq, ReadinessReply, ReadinessReq, RegisterReply, RegisterReq, State,
    UnregisterReply, UnregisterReq, UnwatchReply, UnwatchReq, WatchReply, WatchReq,
};

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
    discovery: Arc<Mutex<Box<dyn Discovery>>>,
}

impl OverseerService {
    pub fn new(discovery: Box<dyn Discovery>) -> OverseerService {
        OverseerService {
            discovery: Arc::new(Mutex::new(discovery)),
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

        let discovery = Arc::clone(&self.discovery);
        let mut discovery = discovery.lock().await;

        match discovery
            .set_node(index_name, shard_name, node_name, node_details)
            .await
        {
            Ok(_) => (),
            Err(e) => {
                timer.observe_duration();

                return Err(Status::new(
                    Code::Internal,
                    format!("failed to remove unnecessary files: error = {:?}", e),
                ));
            }
        };

        let reply = RegisterReply {};

        timer.observe_duration();

        Ok(Response::new(reply))
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

        let discovery = Arc::clone(&self.discovery);
        let mut discovery = discovery.lock().await;

        match discovery
            .delete_node(index_name, shard_name, node_name)
            .await
        {
            Ok(_) => (),
            Err(e) => {
                timer.observe_duration();

                return Err(Status::new(
                    Code::Internal,
                    format!("failed to remove unnecessary files: error = {:?}", e),
                ));
            }
        };

        let reply = UnregisterReply {};

        timer.observe_duration();

        Ok(Response::new(reply))
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

        let discovery = Arc::clone(&self.discovery);
        let mut discovery = discovery.lock().await;

        match discovery.get_node(index_name, shard_name, node_name).await {
            Ok(node_details) => {
                let reply = InquireReply { node_details };

                timer.observe_duration();

                Ok(Response::new(reply))
            }
            Err(e) => {
                timer.observe_duration();

                Err(Status::new(
                    Code::Internal,
                    format!("failed to inquire node details: error = {:?}", e),
                ))
            }
        }
    }

    async fn watch(&self, request: Request<WatchReq>) -> Result<Response<WatchReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["watch"]).inc();
        let timer = REQUEST_HISTOGRAM
            .with_label_values(&["watch"])
            .start_timer();

        let req = request.into_inner();

        let interval = req.interval;

        let discovery = Arc::clone(&self.discovery);
        let mut discovery = discovery.lock().await;

        match discovery.watch().await {
            Ok(_) => (),
            Err(e) => {
                error!("failed to start watch thread: error = {:?}", e);
            }
        };

        match discovery.start_health_check(interval).await {
            Ok(_) => (),
            Err(e) => {
                error!("failed to start health check thread: error = {:?}", e);
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

        let discovery = Arc::clone(&self.discovery);
        let mut discovery = discovery.lock().await;

        match discovery.unwatch().await {
            Ok(_) => (),
            Err(e) => {
                error!("failed to stop watch thread: error = {:?}", e);
            }
        };

        match discovery.stop_health_check().await {
            Ok(_) => (),
            Err(e) => {
                error!("failed to stop health check thread: error = {:?}", e);
            }
        };

        let reply = UnwatchReply {};

        timer.observe_duration();

        Ok(Response::new(reply))
    }
}
