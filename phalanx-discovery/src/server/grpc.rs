use std::sync::Arc;

use lazy_static::lazy_static;
use prometheus::{register_counter_vec, register_histogram_vec, CounterVec, HistogramVec};
use tokio::sync::RwLock;
use tonic::{Code, Request, Response, Status};

use phalanx_proto::phalanx::discovery_service_server::DiscoveryService as ProtoDiscoveryService;
use phalanx_proto::phalanx::{
    ReadinessReply, ReadinessReq, State, UnwatchReply, UnwatchReq, WatchReply, WatchReq,
};

use crate::watcher::Watcher;

lazy_static! {
    static ref REQUEST_COUNTER: CounterVec = register_counter_vec!(
        "phalanx_discovery_requests_total",
        "Total number of requests.",
        &["func"]
    )
    .unwrap();
    static ref REQUEST_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "phalanx_discovery_request_duration_seconds",
        "The request latencies in seconds.",
        &["func"]
    )
    .unwrap();
}

pub struct DiscoveryService {
    watcher: Arc<RwLock<Watcher>>,
}

impl DiscoveryService {
    pub fn new(watcher: Watcher) -> DiscoveryService {
        DiscoveryService {
            watcher: Arc::new(RwLock::new(watcher)),
        }
    }
}

#[tonic::async_trait]
impl ProtoDiscoveryService for DiscoveryService {
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

    async fn watch(&self, _request: Request<WatchReq>) -> Result<Response<WatchReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["watch"]).inc();
        let timer = REQUEST_HISTOGRAM
            .with_label_values(&["watch"])
            .start_timer();

        let watcher_arc = Arc::clone(&self.watcher);
        let mut watcher = watcher_arc.write().await;
        match watcher.watch().await {
            Ok(_) => (),
            Err(e) => {
                timer.observe_duration();

                return Err(Status::new(
                    Code::Internal,
                    format!("failed to watch: error = {:?}", e),
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

        let watcher_arc = Arc::clone(&self.watcher);
        let mut watcher = watcher_arc.write().await;
        match watcher.unwatch().await {
            Ok(_) => (),
            Err(e) => {
                timer.observe_duration();

                return Err(Status::new(
                    Code::Internal,
                    format!("failed to unwatch: error = {:?}", e),
                ));
            }
        };

        let reply = UnwatchReply {};

        timer.observe_duration();

        Ok(Response::new(reply))
    }
}
