use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::error::Error;

use async_std::task::block_on;
use async_trait::async_trait;
use etcd_client::{Client, GetOptions};
use log::*;

use phalanx_proto::phalanx::{NodeDetails, Role, State};

use crate::discovery::Discovery;

pub const TYPE: &str = "etcd";

#[derive(Clone)]
pub struct Etcd {
    pub client: Client,
    pub root: String,
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
        }
    }
}

#[async_trait]
impl Discovery for Etcd {
    fn get_type(&self) -> &str {
        TYPE
    }

    async fn get_indices(&mut self) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        let mut indices: Vec<String> = Vec::new();
        let key = format!("{}/", &self.root);
        match self
            .client
            .get(key.clone(), Some(GetOptions::new().with_prefix()))
            .await
        {
            Ok(get_response) => {
                for kv in get_response.kvs() {
                    let tmp_key = &kv.key_str().unwrap()[&key.len() + 0..];
                    let tmp_name: Vec<&str> = tmp_key.split('/').collect();
                    let index_name = match tmp_name.get(0) {
                        Some(name) => name.clone(),
                        None => {
                            continue;
                        }
                    };
                    if !indices.contains(&index_name.to_string()) {
                        indices.push(index_name.to_string());
                    }
                }
            }
            Err(e) => return Err(Box::new(e)),
        };

        Ok(indices)
    }

    async fn get_shards(
        &mut self,
        index_name: &str,
    ) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        let mut shards: Vec<String> = Vec::new();
        let key = format!("{}/{}/", &self.root, index_name);
        match self
            .client
            .get(key.clone(), Some(GetOptions::new().with_prefix()))
            .await
        {
            Ok(get_response) => {
                for kv in get_response.kvs() {
                    let tmp_key = &kv.key_str().unwrap()[&key.len() + 0..];
                    let tmp_name: Vec<&str> = tmp_key.split('/').collect();
                    let shard_name = match tmp_name.get(0) {
                        Some(name) => name.clone(),
                        None => {
                            continue;
                        }
                    };
                    if !shards.contains(&shard_name.to_string()) {
                        shards.push(shard_name.to_string());
                    }
                }
            }
            Err(e) => return Err(Box::new(e)),
        };

        Ok(shards)
    }

    async fn get_nodes(
        &mut self,
        index_name: &str,
        shard_name: &str,
    ) -> Result<HashMap<String, Option<NodeDetails>>, Box<dyn Error + Send + Sync>> {
        let mut nodes: HashMap<String, Option<NodeDetails>> = HashMap::new();
        let key = format!("{}/{}/{}/", &self.root, index_name, shard_name);
        match self
            .client
            .get(key.clone(), Some(GetOptions::new().with_prefix()))
            .await
        {
            Ok(get_response) => {
                for kv in get_response.kvs() {
                    let node_name = &kv.key_str().unwrap()[&key.len() + 0..];
                    let node_details: NodeDetails =
                        serde_json::from_str(kv.value_str().unwrap()).unwrap();
                    nodes.insert(node_name.to_string(), Some(node_details));
                }
            }
            Err(e) => return Err(Box::new(e)),
        };

        Ok(nodes)
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

    async fn get_primary_node(
        &mut self,
        index_name: &str,
        shard_name: &str,
    ) -> Result<Option<String>, Box<dyn Error + Send + Sync>> {
        match self.get_nodes(index_name, shard_name).await {
            Ok(resp) => {
                let mut node_names = Vec::new();
                for (node_name, node_details) in resp {
                    match node_details {
                        Some(node_details) => {
                            if node_details.state == State::Ready as i32
                                && node_details.role == Role::Primary as i32
                            {
                                node_names.push(node_name);
                            }
                        }
                        None => {
                            // key does not exist
                            debug!("node status does not exist: index_name={}, shard_name={}, node_name={}", index_name, shard_name, &node_name);
                            self.delete_node(index_name, shard_name, &node_name)
                                .await
                                .unwrap();
                        }
                    }
                }

                if node_names.is_empty() {
                    Ok(None)
                } else {
                    if node_names.len() != 1 {
                        warn!("first primary node name returned due to found multiple primary nodes: {:?}", &node_names);
                    }
                    Ok(Some(node_names.get(0).unwrap().to_string()))
                }
            }
            Err(e) => {
                return Err(e);
            }
        }
    }

    async fn get_replica_nodes(
        &mut self,
        index_name: &str,
        shard_name: &str,
    ) -> Result<HashMap<String, Option<NodeDetails>, RandomState>, Box<dyn Error + Send + Sync>>
    {
        let mut nodes: HashMap<String, Option<NodeDetails>> = HashMap::new();

        match self.get_nodes(index_name, shard_name).await {
            Ok(resp) => {
                for (node_name, node_details) in resp {
                    match node_details {
                        Some(node_details) => {
                            if node_details.state == State::Ready as i32
                                && node_details.role == Role::Replica as i32
                            {
                                nodes.insert(node_name, Some(node_details));
                            }
                        }
                        None => {
                            // key does not exist
                            debug!("node status does not exist: index_name={}, shard_name={}, node_name={}", index_name, shard_name, &node_name);
                            self.delete_node(index_name, shard_name, &node_name)
                                .await
                                .unwrap();
                        }
                    }
                }
            }
            Err(e) => {
                return Err(e);
            }
        };

        Ok(nodes)
    }

    async fn get_candidate_nodes(
        &mut self,
        index_name: &str,
        shard_name: &str,
    ) -> Result<HashMap<String, Option<NodeDetails>, RandomState>, Box<dyn Error + Send + Sync>>
    {
        let mut nodes: HashMap<String, Option<NodeDetails>> = HashMap::new();

        match self.get_nodes(index_name, shard_name).await {
            Ok(resp) => {
                for (node_name, node_details) in resp {
                    match node_details {
                        Some(node_details) => {
                            if node_details.state == State::Ready as i32
                                && node_details.role == Role::Candidate as i32
                            {
                                nodes.insert(node_name, Some(node_details));
                            }
                        }
                        None => {
                            // key does not exist
                            debug!("node status does not exist: index_name={}, shard_name={}, node_name={}", index_name, shard_name, &node_name);
                            self.delete_node(index_name, shard_name, &node_name)
                                .await
                                .unwrap();
                        }
                    }
                }
            }
            Err(e) => {
                return Err(e);
            }
        };

        Ok(nodes)
    }
}
