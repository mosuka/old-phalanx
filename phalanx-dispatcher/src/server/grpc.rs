use std::collections::HashMap;

use anyhow::{anyhow, Context, Error, Result};
use futures::future::try_join_all;
use lazy_static::lazy_static;
use log::*;
use prometheus::{register_counter_vec, register_histogram_vec, CounterVec, HistogramVec};
use rand::Rng;
use serde_json::Value;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;
use tonic::codegen::Arc;
use tonic::transport::Channel;
use tonic::Code;
use tonic::{Request, Response, Status};

use phalanx_index::index::search_request::SearchRequest;
use phalanx_index::index::search_result::{ScoredNamedFieldDocument, SearchResult};
use phalanx_proto::phalanx::dispatcher_service_server::DispatcherService as ProtoDispatcherService;
use phalanx_proto::phalanx::index_service_client::IndexServiceClient;
use phalanx_proto::phalanx::{
    BulkDeleteReply, BulkDeleteReq, BulkSetReply, BulkSetReq, CommitReply, CommitReq, DeleteReply,
    DeleteReq, GetReply, GetReq, MergeReply, MergeReq, ReadinessReply, ReadinessReq, Role,
    RollbackReply, RollbackReq, SearchReply, SearchReq, SetReply, SetReq, State, UnwatchReply,
    UnwatchReq, WatchReply, WatchReq,
};

use crate::watcher::Watcher;

lazy_static! {
    static ref REQUEST_COUNTER: CounterVec = register_counter_vec!(
        "phalanx_dispatcher_requests_total",
        "Total number of requests.",
        &["func"]
    )
    .unwrap();
    static ref REQUEST_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "phalanx_dispatcher_request_duration_seconds",
        "The request latencies in seconds.",
        &["func"]
    )
    .unwrap();
}

pub struct DispatcherService {
    watcher: Arc<RwLock<Watcher>>,
}

impl DispatcherService {
    pub async fn new(watcher: Watcher) -> DispatcherService {
        DispatcherService {
            watcher: Arc::new(RwLock::new(watcher)),
        }
    }

    pub async fn get(&self, index_name: &str, id: &str) -> Result<Vec<u8>> {
        let watcher_arc = Arc::clone(&self.watcher);
        let watcher = watcher_arc.read().await;

        let metadata = watcher.metadata_map.read().await;

        let client_map = watcher.client_map.read().await;

        let metadata_shards = match metadata.get(index_name) {
            Some(shards) => shards,
            None => return Err(anyhow!("{} does not exist", index_name)),
        };

        let mut handles: Vec<JoinHandle<Result<Vec<u8>, Error>>> = Vec::new();
        for (shard_name, m_nodes) in metadata_shards {
            let mut primary = "";
            let mut replicas: Vec<&str> = Vec::new();
            for (node, node_details) in m_nodes {
                let state = State::from_i32(node_details.state).unwrap();
                let role = Role::from_i32(node_details.role).unwrap();
                if state == State::Ready {
                    match role {
                        Role::Primary => primary = node,
                        Role::Replica => replicas.push(node),
                        Role::Candidate => {
                            debug!("{} is {:?}", node, role)
                        }
                    }
                }
            }

            debug!("primary: {}", &primary);
            debug!("replica: {:?}", &replicas);

            let node_name;
            if replicas.len() > 0 {
                let idx = rand::thread_rng().gen_range(0, replicas.len());
                node_name = replicas.get(idx).unwrap().clone();
            } else {
                node_name = primary;
            }

            let client_indices = client_map.clone();
            let index_name = index_name.to_string();
            let shard_name = shard_name.to_string();
            let node_name = node_name.to_string();

            let id = id.to_string();

            let handle = tokio::spawn(async move {
                if node_name.len() <= 0 {
                    Err(anyhow!("there is no available primary node"))
                } else {
                    match client_indices.get(&index_name) {
                        Some(client_shards) => match client_shards.get(&shard_name) {
                            Some(client_nodes) => match client_nodes.get(&node_name) {
                                Some(client) => {
                                    let req = tonic::Request::new(GetReq { index_name, id });
                                    let mut client: IndexServiceClient<Channel> = client.clone();
                                    match client.get(req).await {
                                        Ok(resp) => {
                                            let doc = resp.into_inner().doc;
                                            Ok(doc)
                                        }
                                        Err(e) => match e.code() {
                                            Code::NotFound => Ok(Vec::new()),
                                            _ => Err(Error::new(e)),
                                        },
                                    }
                                }
                                None => Err(anyhow!("{} does not exist", node_name)),
                            },
                            None => Err(anyhow!("{} does not exist", shard_name)),
                        },
                        None => Err(anyhow!("{} does not exist", index_name)),
                    }
                }
            });
            handles.push(handle);
        }

        let results = try_join_all(handles)
            .await
            .with_context(|| "failed to join handles")?;
        let results_cnt = results.len();

        let mut ret = Vec::new();
        let mut errs = Vec::new();
        for result in results {
            match result {
                Ok(doc) => {
                    if doc.len() > 0 {
                        ret = doc;
                        break;
                    }
                }
                Err(e) => {
                    error!("{}", e.to_string());
                    errs.push(anyhow!("{}", e.to_string()));
                }
            }
        }

        if results_cnt != errs.len() {
            Ok(ret)
        } else {
            let e = errs.get(0).unwrap().clone();
            Err(anyhow!("{}", e.to_string()))
        }
    }

    pub async fn put(&self, index_name: &str, route_field_name: &str, doc: Vec<u8>) -> Result<()> {
        let value: Value = match serde_json::from_slice::<Value>(&doc.as_slice()) {
            Ok(value) => value,
            Err(e) => {
                return Err(Error::new(e));
            }
        };
        let route_field_value = match value.as_object().unwrap()[route_field_name].as_str() {
            Some(route_field_value) => route_field_value,
            None => {
                return Err(anyhow!("{} does not exist", route_field_name));
            }
        };
        debug!("route field value is {:?}", route_field_value);

        let watcher_arc = Arc::clone(&self.watcher);
        let watcher = watcher_arc.read().await;

        let ring_indices = watcher.shard_ring_map.read().await;
        let shard_ring = match ring_indices.get(index_name) {
            Some(shard_ring) => shard_ring,
            None => return Err(anyhow!("{} does not exist", index_name)),
        };
        let shard_name = shard_ring.get_node(route_field_value.to_string()).unwrap();
        debug!("destination shard: {:?}", shard_name);

        let metadata = watcher.metadata_map.read().await;
        let metadata_nodes = match metadata.get(index_name) {
            Some(shards) => match shards.get(shard_name) {
                Some(nodes) => nodes,
                None => return Err(anyhow!("{} does not exist", shard_name)),
            },
            None => return Err(anyhow!("{} does not exist", index_name)),
        };

        let mut node_name = "";
        for (node, node_details) in metadata_nodes.iter() {
            let state = State::from_i32(node_details.state).unwrap();
            let role = Role::from_i32(node_details.role).unwrap();
            if state == State::Ready && role == Role::Primary {
                node_name = node.as_str();
                break;
            }
        }
        debug!("primary node is {:?}", node_name);

        let client_indices = watcher.client_map.read().await;
        let client = match client_indices.get(index_name) {
            Some(client_shards) => match client_shards.get(shard_name) {
                Some(client_nodes) => match client_nodes.get(node_name) {
                    Some(client) => client.clone(),
                    None => return Err(anyhow!("{} does not exist", node_name)),
                },
                None => return Err(anyhow!("{} does not exist", shard_name)),
            },
            None => return Err(anyhow!("{} does not exist", index_name)),
        };

        let mut client: IndexServiceClient<Channel> = client.into();
        let req = tonic::Request::new(SetReq {
            index_name: index_name.to_string(),
            route_field_name: route_field_name.to_string(),
            doc,
        });
        match client.set(req).await {
            Ok(_resp) => Ok(()),
            Err(e) => Err(Error::new(e)),
        }
    }

    pub async fn delete(&self, index_name: &str, id: &str) -> Result<()> {
        let watcher_arc = Arc::clone(&self.watcher);
        let watcher = watcher_arc.read().await;

        let metadata = watcher.metadata_map.read().await;
        let metadata_shards = match metadata.get(index_name) {
            Some(shards) => shards,
            None => return Err(anyhow!("{} does not exist", index_name)),
        };

        let client_map = watcher.client_map.read().await;
        let mut handles: Vec<JoinHandle<Result<(), Error>>> = Vec::new();
        for (shard_name, nodes) in metadata_shards {
            let mut node_name = "";
            for (node, node_details) in nodes {
                let state = State::from_i32(node_details.state).unwrap();
                let role = Role::from_i32(node_details.role).unwrap();
                if state == State::Ready && role == Role::Primary {
                    node_name = node.as_str();
                    break;
                }
            }
            debug!("primary node is {:?}", node_name);

            let client_indices = client_map.clone();
            let index_name = index_name.to_string();
            let shard_name = shard_name.to_string();
            let node_name = node_name.to_string();
            let id = id.to_string();
            let handle = tokio::spawn(async move {
                if node_name.len() <= 0 {
                    Err(anyhow!("there is no available primary node"))
                } else {
                    match client_indices.get(&index_name) {
                        Some(client_shards) => match client_shards.get(&shard_name) {
                            Some(client_nodes) => match client_nodes.get(&node_name) {
                                Some(client) => {
                                    let req = tonic::Request::new(DeleteReq {
                                        index_name: index_name.clone(),
                                        id,
                                    });
                                    let mut client: IndexServiceClient<Channel> = client.clone();
                                    match client.delete(req).await {
                                        Ok(_resp) => Ok(()),
                                        Err(e) => Err(Error::new(e)),
                                    }
                                }
                                None => Err(anyhow!("{} does not exist", node_name)),
                            },
                            None => Err(anyhow!("{} does not exist", shard_name)),
                        },
                        None => Err(anyhow!("{} does not exist", index_name)),
                    }
                }
            });
            handles.push(handle);
        }

        let results = try_join_all(handles)
            .await
            .with_context(|| "failed to join handles")?;
        let results_cnt = results.len();

        let mut errs = Vec::new();
        for result in results {
            match result {
                Ok(()) => (),
                Err(e) => {
                    error!("{}", &e.to_string());
                    errs.push(e);
                }
            }
        }

        if results_cnt != errs.len() {
            Ok(())
        } else {
            let e = errs.get(0).unwrap().to_string();
            Err(anyhow!(e))
        }
    }

    pub async fn bulk_put(
        &self,
        index_name: &str,
        route_field_name: &str,
        docs: Vec<Vec<u8>>,
    ) -> Result<()> {
        let watcher_arc = Arc::clone(&self.watcher);
        let watcher = watcher_arc.read().await;

        let ring_indices = watcher.shard_ring_map.read().await;
        let shard_ring = match ring_indices.get(index_name) {
            Some(shard_ring) => shard_ring,
            None => return Err(anyhow!("{} does not exist", index_name)),
        };

        let mut docs_map = HashMap::new();
        for doc in docs {
            debug!("{}", String::from_utf8(doc.clone()).unwrap());
            let value: Value = match serde_json::from_slice::<Value>(&doc.as_slice()) {
                Ok(value) => value,
                Err(e) => {
                    return Err(Error::new(e));
                }
            };
            let route_field_value = match value.as_object().unwrap()[route_field_name].as_str() {
                Some(route_field_value) => route_field_value,
                None => {
                    return Err(anyhow!("{} does not exist", route_field_name));
                }
            };
            debug!("route field value is {:?}", route_field_value);

            let shard_name = shard_ring.get_node(route_field_value.to_string()).unwrap();
            debug!("destination shard: {:?}", shard_name);

            if !docs_map.contains_key(shard_name) {
                let docs = Vec::new();
                docs_map.insert(shard_name.clone(), docs);
            }
            docs_map.get_mut(shard_name).unwrap().push(doc);
        }

        let mut handles: Vec<JoinHandle<Result<(), Error>>> = Vec::new();
        let metadata = watcher.metadata_map.read().await;
        let client_map = watcher.client_map.read().await;
        for (shard_name, docs) in docs_map {
            let metadata_nodes = match metadata.get(index_name) {
                Some(shards) => match shards.get(&shard_name) {
                    Some(nodes) => nodes,
                    None => return Err(anyhow!("{} does not exist", shard_name)),
                },
                None => return Err(anyhow!("{} does not exist", index_name)),
            };

            let mut node_name = "";
            for (node, node_details) in metadata_nodes.iter() {
                let state = State::from_i32(node_details.state).unwrap();
                let role = Role::from_i32(node_details.role).unwrap();
                if state == State::Ready && role == Role::Primary {
                    node_name = node.as_str();
                    break;
                }
            }
            debug!("primary node is {:?}", node_name);

            let client_indices = client_map.clone();
            let index_name = index_name.to_string();
            let shard_name = shard_name.to_string();
            let node_name = node_name.to_string();
            let route_field_name = route_field_name.to_string();
            let handle = tokio::spawn(async move {
                if node_name.len() <= 0 {
                    Err(anyhow!("there is no available primary node"))
                } else {
                    match client_indices.get(&index_name) {
                        Some(client_shards) => match client_shards.get(&shard_name) {
                            Some(client_nodes) => match client_nodes.get(&node_name) {
                                Some(client) => {
                                    let req = tonic::Request::new(BulkSetReq {
                                        index_name: index_name.clone(),
                                        route_field_name: route_field_name.clone(),
                                        docs,
                                    });
                                    let mut client: IndexServiceClient<Channel> = client.clone();
                                    match client.bulk_set(req).await {
                                        Ok(_resp) => Ok(()),
                                        Err(e) => Err(Error::new(e)),
                                    }
                                }
                                None => Err(anyhow!("{} does not exist", node_name)),
                            },
                            None => Err(anyhow!("{} does not exist", shard_name)),
                        },
                        None => Err(anyhow!("{} does not exist", index_name)),
                    }
                }
            });
            handles.push(handle);
        }

        let results = try_join_all(handles)
            .await
            .with_context(|| "failed to join handles")?;
        let results_cnt = results.len();

        let mut errs = Vec::new();
        for result in results {
            match result {
                Ok(()) => (),
                Err(e) => {
                    error!("{}", &e.to_string());
                    errs.push(e);
                }
            }
        }

        if results_cnt != errs.len() {
            Ok(())
        } else {
            let e = errs.get(0).unwrap().to_string();
            Err(anyhow!(e))
        }
    }

    pub async fn bulk_delete(&self, index_name: &str, ids: Vec<&str>) -> Result<()> {
        let watcher_arc = Arc::clone(&self.watcher);
        let watcher = watcher_arc.read().await;

        let metadata = watcher.metadata_map.read().await;
        let metadata_shards = match metadata.get(index_name) {
            Some(shards) => shards,
            None => return Err(anyhow!("{} does not exist", index_name)),
        };

        let mut handles: Vec<JoinHandle<Result<(), Error>>> = Vec::new();
        let client_map = watcher.client_map.read().await;
        for (shard_name, nodes) in metadata_shards {
            let mut node_name = "";
            for (node, node_details) in nodes {
                let state = State::from_i32(node_details.state).unwrap();
                let role = Role::from_i32(node_details.role).unwrap();
                if state == State::Ready && role == Role::Primary {
                    node_name = node.as_str();
                    break;
                }
            }
            debug!("primary node is {:?}", node_name);

            let client_indices = client_map.clone();
            let index_name = index_name.to_string();
            let shard_name = shard_name.to_string();
            let node_name = node_name.to_string();
            let ids: Vec<String> = ids.iter().map(|id| id.to_string()).collect();
            let handle = tokio::spawn(async move {
                if node_name.len() <= 0 {
                    Err(anyhow!("there is no available primary node"))
                } else {
                    match client_indices.get(&index_name) {
                        Some(client_shards) => match client_shards.get(&shard_name) {
                            Some(client_nodes) => match client_nodes.get(&node_name) {
                                Some(client) => {
                                    let req = tonic::Request::new(BulkDeleteReq {
                                        index_name: index_name.clone(),
                                        ids,
                                    });
                                    let mut client: IndexServiceClient<Channel> = client.clone();
                                    match client.bulk_delete(req).await {
                                        Ok(_resp) => Ok(()),
                                        Err(e) => Err(Error::new(e)),
                                    }
                                }
                                None => Err(anyhow!("{} does not exist", node_name)),
                            },
                            None => Err(anyhow!("{} does not exist", shard_name)),
                        },
                        None => Err(anyhow!("{} does not exist", index_name)),
                    }
                }
            });
            handles.push(handle);
        }

        let results = try_join_all(handles)
            .await
            .with_context(|| "failed to join handles")?;
        let results_cnt = results.len();

        let mut errs = Vec::new();
        for result in results {
            match result {
                Ok(()) => (),
                Err(e) => {
                    error!("{}", &e.to_string());
                    errs.push(e);
                }
            }
        }

        if results_cnt != errs.len() {
            Ok(())
        } else {
            let e = errs.get(0).unwrap().to_string();
            Err(anyhow!(e))
        }
    }

    pub async fn commit(&self, index_name: &str) -> Result<()> {
        let watcher_arc = Arc::clone(&self.watcher);
        let watcher = watcher_arc.read().await;

        let metadata = watcher.metadata_map.read().await;
        let metadata_shards = match metadata.get(index_name) {
            Some(shards) => shards,
            None => return Err(anyhow!("{} does not exist", index_name)),
        };

        let client_map = watcher.client_map.read().await;
        let mut handles: Vec<JoinHandle<Result<(), Error>>> = Vec::new();
        for (shard_name, nodes) in metadata_shards {
            let mut node_name = "";
            for (node, node_details) in nodes {
                let state = State::from_i32(node_details.state).unwrap();
                let role = Role::from_i32(node_details.role).unwrap();
                if state == State::Ready && role == Role::Primary {
                    node_name = node.as_str();
                    break;
                }
            }
            debug!("primary node is {:?}", node_name);

            let client_indices = client_map.clone();
            let index_name = index_name.to_string();
            let shard_name = shard_name.to_string();
            let node_name = node_name.to_string();
            let handle = tokio::spawn(async move {
                if node_name.len() <= 0 {
                    Err(anyhow!("there is no available primary node"))
                } else {
                    match client_indices.get(&index_name) {
                        Some(client_shards) => match client_shards.get(&shard_name) {
                            Some(client_nodes) => match client_nodes.get(&node_name) {
                                Some(client) => {
                                    let req = tonic::Request::new(CommitReq {
                                        index_name: index_name.clone(),
                                    });
                                    let mut client: IndexServiceClient<Channel> = client.clone();
                                    match client.commit(req).await {
                                        Ok(_resp) => Ok(()),
                                        Err(e) => Err(Error::new(e)),
                                    }
                                }
                                None => Err(anyhow!("{} does not exist", node_name)),
                            },
                            None => Err(anyhow!("{} does not exist", shard_name)),
                        },
                        None => Err(anyhow!("{} does not exist", index_name)),
                    }
                }
            });
            handles.push(handle);
        }

        let results = try_join_all(handles)
            .await
            .with_context(|| "failed to join handles")?;
        let results_cnt = results.len();

        let mut errs = Vec::new();
        for result in results {
            match result {
                Ok(()) => (),
                Err(e) => {
                    error!("{}", &e.to_string());
                    errs.push(e);
                }
            }
        }

        if results_cnt != errs.len() {
            Ok(())
        } else {
            let e = errs.get(0).unwrap().to_string();
            Err(anyhow!(e))
        }
    }

    pub async fn rollback(&self, index_name: &str) -> Result<()> {
        let watcher_arc = Arc::clone(&self.watcher);
        let watcher = watcher_arc.read().await;

        let metadata = watcher.metadata_map.read().await;
        let metadata_shards = match metadata.get(index_name) {
            Some(shards) => shards,
            None => return Err(anyhow!("{} does not exist", index_name)),
        };

        let mut handles: Vec<JoinHandle<Result<(), Error>>> = Vec::new();
        let client_map = watcher.client_map.read().await;
        for (shard_name, nodes) in metadata_shards {
            let mut node_name = "";
            for (node, node_details) in nodes {
                let state = State::from_i32(node_details.state).unwrap();
                let role = Role::from_i32(node_details.role).unwrap();
                if state == State::Ready && role == Role::Primary {
                    node_name = node.as_str();
                    break;
                }
            }
            debug!("primary node is {:?}", node_name);

            let client_indices = client_map.clone();
            let index_name = index_name.to_string();
            let shard_name = shard_name.to_string();
            let node_name = node_name.to_string();
            let handle = tokio::spawn(async move {
                if node_name.len() <= 0 {
                    Err(anyhow!("there is no available primary node"))
                } else {
                    match client_indices.get(&index_name) {
                        Some(client_shards) => match client_shards.get(&shard_name) {
                            Some(client_nodes) => match client_nodes.get(&node_name) {
                                Some(client) => {
                                    let req = tonic::Request::new(RollbackReq {
                                        index_name: index_name.clone(),
                                    });
                                    let mut client: IndexServiceClient<Channel> = client.clone();
                                    match client.rollback(req).await {
                                        Ok(_resp) => Ok(()),
                                        Err(e) => Err(Error::new(e)),
                                    }
                                }
                                None => Err(anyhow!("{} does not exist", node_name)),
                            },
                            None => Err(anyhow!("{} does not exist", shard_name)),
                        },
                        None => Err(anyhow!("{} does not exist", index_name)),
                    }
                }
            });
            handles.push(handle);
        }

        let results = try_join_all(handles)
            .await
            .with_context(|| "failed to join handles")?;
        let results_cnt = results.len();

        let mut errs = Vec::new();
        for result in results {
            match result {
                Ok(()) => (),
                Err(e) => {
                    error!("{}", &e.to_string());
                    errs.push(e);
                }
            }
        }

        if results_cnt != errs.len() {
            Ok(())
        } else {
            let e = errs.get(0).unwrap().to_string();
            Err(anyhow!(e))
        }
    }

    pub async fn merge(&self, index_name: &str) -> Result<()> {
        let watcher_arc = Arc::clone(&self.watcher);
        let watcher = watcher_arc.read().await;

        let metadata = watcher.metadata_map.read().await;
        let metadata_shards = match metadata.get(index_name) {
            Some(shards) => shards,
            None => return Err(anyhow!("{} does not exist", index_name)),
        };

        let mut handles: Vec<JoinHandle<Result<(), Error>>> = Vec::new();
        let client_map = watcher.client_map.read().await;
        for (shard_name, nodes) in metadata_shards {
            let mut node_name = "";
            for (node, node_details) in nodes {
                let state = State::from_i32(node_details.state).unwrap();
                let role = Role::from_i32(node_details.role).unwrap();
                if state == State::Ready && role == Role::Primary {
                    node_name = node.as_str();
                    break;
                }
            }
            debug!("primary node is {:?}", node_name);

            let client_indices = client_map.clone();
            let index_name = index_name.to_string();
            let shard_name = shard_name.to_string();
            let node_name = node_name.to_string();
            let handle = tokio::spawn(async move {
                if node_name.len() <= 0 {
                    Err(anyhow!("there is no available primary node"))
                } else {
                    match client_indices.get(&index_name) {
                        Some(client_shards) => match client_shards.get(&shard_name) {
                            Some(client_nodes) => match client_nodes.get(&node_name) {
                                Some(client) => {
                                    let req = tonic::Request::new(MergeReq {
                                        index_name: index_name.clone(),
                                    });
                                    let mut client: IndexServiceClient<Channel> = client.clone();
                                    match client.merge(req).await {
                                        Ok(_resp) => Ok(()),
                                        Err(e) => Err(Error::new(e)),
                                    }
                                }
                                None => Err(anyhow!("{} does not exist", node_name)),
                            },
                            None => Err(anyhow!("{} does not exist", shard_name)),
                        },
                        None => Err(anyhow!("{} does not exist", index_name)),
                    }
                }
            });
            handles.push(handle);
        }

        let results = try_join_all(handles)
            .await
            .with_context(|| "failed to join handles")?;
        let results_cnt = results.len();

        let mut errs = Vec::new();
        for result in results {
            match result {
                Ok(()) => (),
                Err(e) => {
                    error!("{}", &e.to_string());
                    errs.push(e);
                }
            }
        }

        if results_cnt != errs.len() {
            Ok(())
        } else {
            let e = errs.get(0).unwrap().to_string();
            Err(anyhow!(e))
        }
    }

    pub async fn search(&self, index_name: &str, request: Vec<u8>) -> Result<Vec<u8>> {
        let search_request = match serde_json::from_slice::<SearchRequest>(request.as_slice()) {
            Ok(search_request) => search_request,
            Err(e) => return Err(anyhow!("{}", e.to_string())),
        };
        debug!("search_request={:?}", &search_request);

        let mut distrib_search_request = search_request.clone();
        distrib_search_request.limit = distrib_search_request.limit + distrib_search_request.from;
        distrib_search_request.from = 0;
        debug!("distrib_search_request={:?}", &distrib_search_request);

        let distrib_request = match serde_json::to_vec(&distrib_search_request) {
            Ok(request) => request,
            Err(e) => return Err(anyhow!("{}", e.to_string())),
        };

        let watcher_arc = Arc::clone(&self.watcher);
        let watcher = watcher_arc.read().await;

        let metadata = watcher.metadata_map.read().await;
        let metadata_shards = match metadata.get(index_name) {
            Some(shards) => shards,
            None => return Err(anyhow!("{} does not exist", index_name)),
        };

        let mut handles: Vec<JoinHandle<Result<Vec<u8>, Error>>> = Vec::new();
        let client_map = watcher.client_map.read().await;
        for (shard_name, m_nodes) in metadata_shards {
            let mut primary = "";
            let mut replicas: Vec<&str> = Vec::new();
            for (node, node_details) in m_nodes {
                let state = State::from_i32(node_details.state).unwrap();
                let role = Role::from_i32(node_details.role).unwrap();
                if state == State::Ready {
                    match role {
                        Role::Primary => primary = node,
                        Role::Replica => replicas.push(node),
                        Role::Candidate => {
                            debug!("{} is {:?}", node, role)
                        }
                    }
                }
            }

            debug!("primary: {}", &primary);
            debug!("replica: {:?}", &replicas);

            let node_name;
            if replicas.len() > 0 {
                let idx = rand::thread_rng().gen_range(0, replicas.len());
                node_name = replicas.get(idx).unwrap().clone();
            } else {
                node_name = primary;
            }

            let client_indices = client_map.clone();
            let index_name = index_name.to_string();
            let shard_name = shard_name.to_string();
            let node_name = node_name.to_string();
            let distrib_request = distrib_request.clone();
            let handle = tokio::spawn(async move {
                if node_name.len() <= 0 {
                    Err(anyhow!("there is no available primary node"))
                } else {
                    match client_indices.get(&index_name) {
                        Some(client_shards) => match client_shards.get(&shard_name) {
                            Some(client_nodes) => match client_nodes.get(&node_name) {
                                Some(client) => {
                                    let req = tonic::Request::new(SearchReq {
                                        index_name: index_name.clone(),
                                        request: distrib_request,
                                    });
                                    let mut client: IndexServiceClient<Channel> = client.clone();
                                    match client.search(req).await {
                                        Ok(resp) => {
                                            let result = resp.into_inner().result;
                                            Ok(result)
                                        }
                                        Err(e) => match e.code() {
                                            Code::NotFound => Ok(Vec::new()),
                                            _ => Err(Error::new(e)),
                                        },
                                    }
                                }
                                None => Err(anyhow!("{} does not exist", node_name)),
                            },
                            None => Err(anyhow!("{} does not exist", shard_name)),
                        },
                        None => Err(anyhow!("{} does not exist", index_name)),
                    }
                }
            });
            handles.push(handle);
        }

        let responses = try_join_all(handles)
            .await
            .with_context(|| "failed to join handles")?;
        let resp_cnt = responses.len();

        let mut count = 0;
        let mut docs = Vec::new();
        let mut facet = HashMap::new();

        let mut errs = Vec::new();
        for response in responses {
            match response {
                Ok(result) => {
                    let search_result =
                        match serde_json::from_slice::<SearchResult>(result.as_slice()) {
                            Ok(search_result) => search_result,
                            Err(e) => {
                                error!("{}", e.to_string());
                                errs.push(anyhow!("{}", e.to_string()));
                                continue;
                            }
                        };

                    // increment hit count
                    count += search_result.count;

                    // add docs
                    for doc in search_result.docs {
                        docs.push(doc);
                    }

                    // add facets
                    for (facet_field, facet_data) in search_result.facet {
                        if !facet.contains_key(&facet_field) {
                            facet.insert(facet_field.clone(), HashMap::new());
                        }
                        for (facet_value, facet_cnt) in facet_data {
                            if !facet.get(&facet_field).unwrap().contains_key(&facet_value) {
                                facet
                                    .get_mut(&facet_field)
                                    .unwrap()
                                    .insert(facet_value, facet_cnt);
                            } else {
                                let new_facet_cnt =
                                    facet.get(&facet_field).unwrap().get(&facet_value).unwrap()
                                        + facet_cnt;
                                facet
                                    .get_mut(&facet_field)
                                    .unwrap()
                                    .insert(facet_value, new_facet_cnt);
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("{}", e.to_string());
                    errs.push(anyhow!("{}", e.to_string()));
                }
            }
        }

        // sort by score
        docs.sort_by(|r1, r2| {
            // r1.score.partial_cmp(&r2.score).unwrap() // asc
            r1.score.partial_cmp(&r2.score).unwrap().reverse() // desc
        });

        // trimming
        let start = search_request.from as usize;
        let mut end = search_request.limit as usize + start;
        if docs.len() < end {
            end = docs.len();
        }
        // docs = docs[start..end].to_vec();  // need to implement `Clone`
        let json = serde_json::to_vec(&docs[start..end]).unwrap();
        docs = serde_json::from_slice::<Vec<ScoredNamedFieldDocument>>(json.as_slice()).unwrap();

        let ret_result = SearchResult { count, docs, facet };

        if resp_cnt != errs.len() {
            let ret = serde_json::to_vec(&ret_result).unwrap();
            Ok(ret)
        } else {
            let e = errs.get(0).unwrap().clone();
            Err(anyhow!("{}", e.to_string()))
        }
    }
}

#[tonic::async_trait]
impl ProtoDispatcherService for DispatcherService {
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
            Err(err) => {
                timer.observe_duration();

                return Err(Status::new(
                    Code::Internal,
                    format!("failed to watch: error = {:?}", err),
                ));
            }
        }

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
            Err(err) => {
                timer.observe_duration();

                return Err(Status::new(
                    Code::Internal,
                    format!("failed to unwatch: error = {:?}", err),
                ));
            }
        }

        let reply = UnwatchReply {};

        timer.observe_duration();

        Ok(Response::new(reply))
    }

    async fn get(&self, request: Request<GetReq>) -> Result<Response<GetReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["get"]).inc();
        let timer = REQUEST_HISTOGRAM.with_label_values(&["get"]).start_timer();

        info!("get {:?}", request);

        let req = request.into_inner();

        match self.get(&req.index_name, &req.id).await {
            Ok(resp) => {
                let reply = GetReply { doc: resp };

                timer.observe_duration();

                Ok(Response::new(reply))
            }
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

        info!("put {:?}", request);

        let req = request.into_inner();

        match self
            .put(&req.index_name, &req.route_field_name, req.doc)
            .await
        {
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

        info!("delete {:?}", request);

        let req = request.into_inner();

        match self.delete(&req.index_name, &req.id).await {
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

        info!("bulk_put {:?}", request);

        let req = request.into_inner();

        match self
            .bulk_put(&req.index_name, &req.route_field_name, req.docs)
            .await
        {
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

        info!("bulk_delete {:?}", request);

        let req = request.into_inner();

        let ids = req.ids.iter().map(|id| id.as_str()).collect();
        match self.bulk_delete(&req.index_name, ids).await {
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

    async fn commit(&self, request: Request<CommitReq>) -> Result<Response<CommitReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["commit"]).inc();
        let timer = REQUEST_HISTOGRAM
            .with_label_values(&["commit"])
            .start_timer();

        info!("commit {:?}", request);

        let req = request.into_inner();

        match self.commit(&req.index_name).await {
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
    }

    async fn rollback(
        &self,
        request: Request<RollbackReq>,
    ) -> Result<Response<RollbackReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["rollback"]).inc();
        let timer = REQUEST_HISTOGRAM
            .with_label_values(&["rollback"])
            .start_timer();

        info!("rollback {:?}", request);

        let req = request.into_inner();

        match self.rollback(&req.index_name).await {
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

    async fn merge(&self, request: Request<MergeReq>) -> Result<Response<MergeReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["merge"]).inc();
        let timer = REQUEST_HISTOGRAM
            .with_label_values(&["merge"])
            .start_timer();

        info!("merge {:?}", request);

        let req = request.into_inner();

        match self.merge(&req.index_name).await {
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
    }

    async fn search(&self, request: Request<SearchReq>) -> Result<Response<SearchReply>, Status> {
        REQUEST_COUNTER.with_label_values(&["search"]).inc();
        let timer = REQUEST_HISTOGRAM
            .with_label_values(&["search"])
            .start_timer();

        info!("search {:?}", request);

        let req = request.into_inner();

        match self.search(&req.index_name, req.request).await {
            Ok(result) => {
                let reply = SearchReply { result };

                timer.observe_duration();

                Ok(Response::new(reply))
            }
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
