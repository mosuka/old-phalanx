use lazy_static::lazy_static;
use prometheus::{register_counter_vec, register_histogram_vec, CounterVec, HistogramVec};
use tonic::{Code, Request, Response, Status};

use phalanx_proto::phalanx::index_service_server::IndexService as ProtoIndexService;
use phalanx_proto::phalanx::{
    BulkDeleteReply, BulkDeleteReq, BulkSetReply, BulkSetReq, CommitReply, CommitReq, DeleteReply,
    DeleteReq, GetReply, GetReq, MergeReply, MergeReq, PullReply, PullReq, PushReply, PushReq,
    ReadinessReply, ReadinessReq, RollbackReply, RollbackReq, SchemaReply, SchemaReq, SearchReply,
    SearchReq, SetReply, SetReq, State, UnwatchReply, UnwatchReq, WatchReply, WatchReq,
};

use crate::index::search_request::SearchRequest;
use crate::index::Index as PIndex;

lazy_static! {
    static ref REQUEST_COUNTER: CounterVec = register_counter_vec!(
        "phalanx_index_requests_total",
        "Total number of requests.",
        &["func"]
    )
    .unwrap();
    static ref REQUEST_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "phalanx_index_request_duration_seconds",
        "The request latencies in seconds.",
        &["func"]
    )
    .unwrap();
}

pub struct IndexService {
    index: PIndex,
}

impl IndexService {
    pub fn new(index: PIndex) -> IndexService {
        IndexService { index }
    }
}

#[tonic::async_trait]
impl ProtoIndexService for IndexService {
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

        // let discovery = Arc::clone(&self.discovery);
        // let mut discovery = discovery.lock().await;
        //
        // match discovery.watch_cluster().await {
        //     Ok(_) => (),
        //     Err(e) => {
        //         error!("failed to start watch thread: error = {:?}", e);
        //     }
        // };

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

        // let discovery = Arc::clone(&self.discovery);
        // let mut discovery = discovery.lock().await;
        //
        // match discovery.unwatch_cluster().await {
        //     Ok(_) => (),
        //     Err(e) => {
        //         error!("failed to stop watch thread: error = {:?}", e);
        //     }
        // };

        let reply = UnwatchReply {};

        timer.observe_duration();

        Ok(Response::new(reply))
    }

    async fn get(&self, request: Request<GetReq>) -> Result<Response<GetReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["get"]).inc();
        let timer = REQUEST_HISTOGRAM.with_label_values(&["get"]).start_timer();

        let req = request.into_inner();

        match self.index.get(&req.id).await {
            Ok(resp) => match resp {
                Some(json) => {
                    let reply = GetReply { doc: json };

                    timer.observe_duration();

                    Ok(Response::new(reply))
                }
                None => {
                    timer.observe_duration();

                    Err(Status::new(Code::InvalidArgument, "document is None"))
                }
            },
            Err(e) => {
                timer.observe_duration();

                Err(Status::new(
                    Code::Internal,
                    format!("failed to get document: error = {:?}", e),
                ))
            }
        }
    }

    async fn set(&self, request: Request<SetReq>) -> Result<Response<SetReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["set"]).inc();
        let timer = REQUEST_HISTOGRAM.with_label_values(&["set"]).start_timer();

        let req = request.into_inner();

        match self.index.set(&req.doc).await {
            Ok(_) => {
                let reply = SetReply {};

                timer.observe_duration();
                Ok(Response::new(reply))
            }
            Err(e) => {
                timer.observe_duration();

                Err(Status::new(
                    Code::InvalidArgument,
                    format!("failed to set document: err = {:?}", e),
                ))
            }
        }
    }

    async fn delete(&self, request: Request<DeleteReq>) -> Result<Response<DeleteReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["delete"]).inc();
        let timer = REQUEST_HISTOGRAM
            .with_label_values(&["delete"])
            .start_timer();

        let req = request.into_inner();

        match self.index.delete(&req.id).await {
            Ok(_) => {
                let reply = DeleteReply {};

                timer.observe_duration();
                Ok(Response::new(reply))
            }
            Err(e) => {
                timer.observe_duration();

                Err(Status::new(
                    Code::InvalidArgument,
                    format!("failed to delete document: err = {:?}", e),
                ))
            }
        }
    }

    async fn bulk_set(
        &self,
        request: Request<BulkSetReq>,
    ) -> Result<Response<BulkSetReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["bulk_set"]).inc();
        let timer = REQUEST_HISTOGRAM
            .with_label_values(&["bulk_set"])
            .start_timer();

        let req = request.into_inner();

        let docs = req.docs.iter().map(|doc| doc.as_str()).collect();
        match self.index.bulk_set(docs).await {
            Ok(_) => {
                let reply = BulkSetReply {};

                timer.observe_duration();
                Ok(Response::new(reply))
            }
            Err(e) => {
                timer.observe_duration();

                Err(Status::new(
                    Code::InvalidArgument,
                    format!("failed to set documents in bulk: err = {:?}", e),
                ))
            }
        }
    }

    async fn bulk_delete(
        &self,
        request: Request<BulkDeleteReq>,
    ) -> Result<Response<BulkDeleteReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["bulk_delete"]).inc();
        let timer = REQUEST_HISTOGRAM
            .with_label_values(&["bulk_delete"])
            .start_timer();

        let req = request.into_inner();

        let ids = req.ids.iter().map(|id| id.as_str()).collect();
        match self.index.bulk_delete(ids).await {
            Ok(_) => {
                let reply = BulkDeleteReply {};

                timer.observe_duration();
                Ok(Response::new(reply))
            }
            Err(e) => {
                timer.observe_duration();

                Err(Status::new(
                    Code::InvalidArgument,
                    format!("failed to delete documents in bulk: err = {:?}", e),
                ))
            }
        }
    }

    async fn commit(&self, _request: Request<CommitReq>) -> Result<Response<CommitReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["commit"]).inc();
        let timer = REQUEST_HISTOGRAM
            .with_label_values(&["commit"])
            .start_timer();

        match self.index.commit().await {
            Ok(_) => {
                let reply = CommitReply {};

                timer.observe_duration();

                Ok(Response::new(reply))
            }
            Err(e) => {
                timer.observe_duration();

                Err(Status::new(
                    Code::Internal,
                    format!("failed to commit index: error = {:?}", e),
                ))
            }
        }

        // // push index
        // let discovery_container = Arc::clone(&self.discovery_container);
        // let mut discovery_container = discovery_container.lock().await;
        // let key = format!(
        //     "/{}/{}/{}/{}.json",
        //     CLUSTER_PATH, &self.index_name, &self.shard_name, &self.node_name
        // );
        // match discovery_container.discovery.get(key.as_str()).await {
        //     Ok(result) => match result {
        //         Some(s) => match serde_json::from_str::<NodeDetails>(s.as_str()) {
        //             Ok(node_details) => {
        //                 if node_details.role == Role::Primary as i32 {
        //                     let storage = Arc::clone(&self.storage);
        //                     let storage = storage.lock().await;
        //                     match storage.push(&self.index_name, &self.shard_name).await {
        //                         Ok(_) => (),
        //                         Err(e) => {
        //                             timer.observe_duration();
        //
        //                             return Err(Status::new(
        //                                 Code::Internal,
        //                                 format!("failed to push index: error = {:?}", e),
        //                             ));
        //                         }
        //                     }
        //                 } else {
        //                     debug!("I'm replica node");
        //                 };
        //             }
        //             Err(e) => {
        //                 timer.observe_duration();
        //
        //                 return Err(Status::new(
        //                     Code::Internal,
        //                     format!("failed to parse content: error = {:?}", e),
        //                 ));
        //             }
        //         },
        //         None => debug!("the node does not exist"),
        //     },
        //     Err(e) => {
        //         timer.observe_duration();
        //
        //         return Err(Status::new(
        //             Code::Internal,
        //             format!("failed to get node: error = {:?}", e),
        //         ));
        //     }
        // };
    }

    async fn rollback(
        &self,
        _request: Request<RollbackReq>,
    ) -> Result<Response<RollbackReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["rollback"]).inc();
        let timer = REQUEST_HISTOGRAM
            .with_label_values(&["rollback"])
            .start_timer();

        match self.index.rollback().await {
            Ok(_) => {
                let reply = RollbackReply {};

                timer.observe_duration();

                Ok(Response::new(reply))
            }
            Err(e) => {
                timer.observe_duration();

                Err(Status::new(
                    Code::Internal,
                    format!("failed to rollback index: error = {:?}", e),
                ))
            }
        }
    }

    async fn merge(&self, _request: Request<MergeReq>) -> Result<Response<MergeReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["merge"]).inc();
        let timer = REQUEST_HISTOGRAM
            .with_label_values(&["merge"])
            .start_timer();

        match self.index.merge().await {
            Ok(_) => {
                let reply = MergeReply {};

                timer.observe_duration();

                Ok(Response::new(reply))
            }
            Err(e) => {
                timer.observe_duration();

                Err(Status::new(
                    Code::Internal,
                    format!("failed to merge index: error = {:?}", e),
                ))
            }
        }

        // // push index
        // let discovery_container = Arc::clone(&self.discovery_container);
        // let mut discovery_container = discovery_container.lock().await;
        // let key = format!(
        //     "/{}/{}/{}/{}.json",
        //     CLUSTER_PATH, &self.index_name, &self.shard_name, &self.node_name
        // );
        // match discovery_container.discovery.get(key.as_str()).await {
        //     Ok(result) => match result {
        //         Some(s) => match serde_json::from_str::<NodeDetails>(s.as_str()) {
        //             Ok(node_details) => {
        //                 if node_details.role == Role::Primary as i32 {
        //                     let storage = Arc::clone(&self.storage);
        //                     let storage = storage.lock().await;
        //                     match storage.push(&self.index_name, &self.shard_name).await {
        //                         Ok(_) => (),
        //                         Err(e) => {
        //                             timer.observe_duration();
        //
        //                             return Err(Status::new(
        //                                 Code::Internal,
        //                                 format!("failed to push index: error = {:?}", e),
        //                             ));
        //                         }
        //                     }
        //                 } else {
        //                     debug!("I'm replica node");
        //                 };
        //             }
        //             Err(e) => {
        //                 timer.observe_duration();
        //
        //                 return Err(Status::new(
        //                     Code::Internal,
        //                     format!("failed to parse content: error = {:?}", e),
        //                 ));
        //             }
        //         },
        //         None => debug!("the node does not exist"),
        //     },
        //     Err(e) => {
        //         timer.observe_duration();
        //
        //         return Err(Status::new(
        //             Code::Internal,
        //             format!("failed to get node: error = {:?}", e),
        //         ));
        //     }
        // };
    }

    async fn push(&self, _request: Request<PushReq>) -> Result<Response<PushReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["push"]).inc();
        let timer = REQUEST_HISTOGRAM.with_label_values(&["push"]).start_timer();

        match self.index.push().await {
            Ok(_) => {
                let reply = PushReply {};

                timer.observe_duration();

                Ok(Response::new(reply))
            }
            Err(e) => {
                timer.observe_duration();

                Err(Status::new(
                    Code::Internal,
                    format!("failed to push index: error = {:?}", e),
                ))
            }
        }
    }

    async fn pull(&self, _request: Request<PullReq>) -> Result<Response<PullReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["pull"]).inc();
        let timer = REQUEST_HISTOGRAM.with_label_values(&["pull"]).start_timer();

        match self.index.pull().await {
            Ok(_) => {
                let reply = PullReply {};

                timer.observe_duration();

                Ok(Response::new(reply))
            }
            Err(e) => {
                timer.observe_duration();

                Err(Status::new(
                    Code::Internal,
                    format!("failed to pull index: error = {:?}", e),
                ))
            }
        }
    }

    async fn schema(&self, _request: Request<SchemaReq>) -> Result<Response<SchemaReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["schema"]).inc();
        let timer = REQUEST_HISTOGRAM
            .with_label_values(&["schema"])
            .start_timer();

        match serde_json::to_string(&self.index.schema()) {
            Ok(schema) => {
                let reply = SchemaReply { schema };

                timer.observe_duration();

                Ok(Response::new(reply))
            }
            Err(e) => {
                timer.observe_duration();

                Err(Status::new(
                    Code::Internal,
                    format!("failed to get schema: error = {:?}", e),
                ))
            }
        }
    }

    async fn search(&self, request: Request<SearchReq>) -> Result<Response<SearchReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["search"]).inc();
        let timer = REQUEST_HISTOGRAM
            .with_label_values(&["search"])
            .start_timer();

        let req = request.into_inner();

        let search_request = match serde_json::from_str::<SearchRequest>(&req.request) {
            Ok(search_request) => search_request,
            Err(e) => {
                timer.observe_duration();

                return Err(Status::new(
                    Code::Internal,
                    format!("failed to parse search request: error = {:?}", e),
                ));
            }
        };

        match self.index.search(search_request).await {
            Ok(result) => match serde_json::to_string(&result) {
                Ok(json) => {
                    let reply = SearchReply { result: json };

                    timer.observe_duration();

                    Ok(Response::new(reply))
                }
                Err(e) => {
                    timer.observe_duration();

                    Err(Status::new(
                        Code::Internal,
                        format!("failed to serialize to JSON: error = {:?}", e),
                    ))
                }
            },
            Err(e) => {
                timer.observe_duration();

                Err(Status::new(
                    Code::Internal,
                    format!("failed to search index: error = {:?}", e),
                ))
            }
        }
    }
}
