use std::convert::TryFrom;
use std::error::Error;
use std::io::{Error as IOError, ErrorKind};

use async_std::task::block_on;
use async_trait::async_trait;
use etcd_client::Client;
use log::*;
use serde::Serialize;

use crate::discovery::{Discovery, NodeStatus, State};

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
        cluster: &str,
        shard: &str,
        node: &str,
        node_status: NodeStatus,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        info!(
            "set_node: cluster={}, shard={}, node={}, node_status={:?}",
            cluster, shard, node, node_status
        );

        let key_bytes = format!("{}/{}/{}/{}", &self.root, cluster, shard, node).into_bytes();
        let value_bytes = serde_json::to_string(&node_status).unwrap().into_bytes();

        let put_response = match self.client.put(key_bytes, value_bytes, None).await {
            Ok(put_response) => put_response,
            Err(e) => return Err(Box::try_from(e).unwrap()),
        };

        Ok(())
    }

    async fn delete_node(
        &mut self,
        cluster: &str,
        shard: &str,
        node: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        info!(
            "delete_node: cluster={}, shard={}, node={}",
            cluster, shard, node
        );

        let key_bytes = format!("{}/{}/{}/{}", &self.root, cluster, shard, node).into_bytes();
        let delete_response = match self.client.delete(key_bytes, None).await {
            Ok(delete_response) => delete_response,
            Err(e) => return Err(Box::try_from(e).unwrap()),
        };

        Ok(())
    }

    async fn get_node(
        &mut self,
        cluster: &str,
        shard: &str,
        node: &str,
    ) -> Result<NodeStatus, Box<dyn Error + Send + Sync>> {
        info!(
            "get_node: cluster={}, shard={}, node={}",
            cluster, shard, node
        );

        let key_bytes = format!("{}/{}/{}/{}", &self.root, cluster, shard, node).into_bytes();
        let get_response = match self.client.get(key_bytes, None).await {
            Ok(get_response) => get_response,
            Err(e) => return Err(Box::try_from(e).unwrap()),
        };

        match get_response.kvs().first() {
            Some(kv) => {
                let node_status: NodeStatus =
                    serde_json::from_str(kv.value_str().unwrap()).unwrap();
                Ok(node_status)
            }
            _ => Err(Box::try_from(IOError::new(
                ErrorKind::NotFound,
                format!(
                    "key does not found: {}",
                    format!("/{}/{}/{}/{}", "phalanx", cluster, shard, node)
                ),
            ))
            .unwrap()),
        }
    }
}
