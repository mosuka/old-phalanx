use std::collections::HashMap;
use std::error::Error;
use std::io::{Error as IOError, ErrorKind};

use async_std::task::block_on;
use async_trait::async_trait;
use etcd_client::{Client, GetOptions};
use log::*;

use crate::discovery::{Discovery, NodeKey, NodeStatus};

pub const DISCOVERY_TYPE: &str = "etcd";

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
        DISCOVERY_TYPE
    }

    async fn set_node(
        &mut self,
        node_key: NodeKey,
        node_status: NodeStatus,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        debug!(
            "set_node: node_key={:?}, node_status={:?}",
            node_key, node_status
        );

        let key = format!(
            "{}/{}/{}/{}",
            &self.root, &node_key.index_name, &node_key.shard_name, &node_key.node_name
        );
        let value = serde_json::to_string(&node_status).unwrap();

        match self.client.put(key, value, None).await {
            Ok(_put_response) => Ok(()),
            Err(e) => Err(Box::new(e)),
        }
    }

    async fn delete_node(&mut self, node_key: NodeKey) -> Result<(), Box<dyn Error + Send + Sync>> {
        debug!("delete_node: node_key={:?}", node_key);

        let key = format!(
            "{}/{}/{}/{}",
            &self.root, &node_key.index_name, &node_key.shard_name, &node_key.node_name
        );
        match self.client.delete(key, None).await {
            Ok(_delete_response) => Ok(()),
            Err(e) => Err(Box::new(e)),
        }
    }

    async fn get_node(
        &mut self,
        node_key: NodeKey,
    ) -> Result<NodeStatus, Box<dyn Error + Send + Sync>> {
        debug!("get_node: node_key={:?}", node_key);

        let key = format!(
            "{}/{}/{}/{}",
            &self.root, &node_key.index_name, &node_key.shard_name, &node_key.node_name
        );
        let get_response = match self.client.get(key.clone(), None).await {
            Ok(get_response) => get_response,
            Err(e) => return Err(Box::new(e)),
        };

        match get_response.kvs().first() {
            Some(kv) => {
                let node_status: NodeStatus =
                    serde_json::from_str(kv.value_str().unwrap()).unwrap();
                Ok(node_status)
            }
            None => Err(Box::new(IOError::new(
                ErrorKind::NotFound,
                format!("key does not fount: {}", &key),
            ))),
        }
    }

    async fn get_nodes(
        &mut self,
    ) -> Result<HashMap<NodeKey, NodeStatus>, Box<dyn Error + Send + Sync>> {
        debug!("get_nodes");

        let mut nodes: HashMap<NodeKey, NodeStatus> = HashMap::new();

        let key = format!("{}/", &self.root);
        match self
            .client
            .get(key, Some(GetOptions::new().with_prefix()))
            .await
        {
            Ok(get_response) => {
                for kv in get_response.kvs() {
                    // get key excludes root dir
                    // e.g. /phalanx/index0/shard0/node0 to index0/shard0/node0
                    let key_str = &kv.key_str().unwrap()[self.root.len() + 1..];

                    let keys: Vec<&str> = key_str.split('/').collect();

                    let node_key = NodeKey {
                        index_name: keys.get(0).unwrap().to_string(),
                        shard_name: keys.get(1).unwrap().to_string(),
                        node_name: keys.get(2).unwrap().to_string(),
                    };
                    let node_status: NodeStatus =
                        serde_json::from_str(kv.value_str().unwrap()).unwrap();
                    nodes.insert(node_key, node_status);
                }
            }
            Err(e) => return Err(Box::new(e)),
        };

        Ok(nodes)
    }
}
