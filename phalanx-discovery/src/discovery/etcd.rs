use std::error::Error;
use std::io::{Error as IOError, ErrorKind};

use async_std::task::block_on;
use async_trait::async_trait;
use etcd_client::Client;
use log::*;

use crate::discovery::{Discovery, NodeStatus};

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
        index_name: &str,
        shard_name: &str,
        node_name: &str,
        node_status: NodeStatus,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        info!(
            "set_node: index_name={}, shard_name={}, node_name={}, node_status={:?}",
            index_name, shard_name, node_name, node_status
        );

        let key = format!("{}/{}/{}/{}", &self.root, index_name, shard_name, node_name);
        let value = serde_json::to_string(&node_status).unwrap();

        let _put_response = match self.client.put(key, value, None).await {
            Ok(put_response) => put_response,
            Err(e) => return Err(Box::new(e)),
        };

        Ok(())
    }

    async fn delete_node(
        &mut self,
        index_name: &str,
        shard_name: &str,
        node_name: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        info!(
            "delete_node: index_name={}, shard_name={}, node_name={}",
            index_name, shard_name, node_name
        );

        let key = format!("{}/{}/{}/{}", &self.root, index_name, shard_name, node_name);
        let _delete_response = match self.client.delete(key, None).await {
            Ok(delete_response) => delete_response,
            Err(e) => return Err(Box::new(e)),
        };

        Ok(())
    }

    async fn get_node(
        &mut self,
        index_name: &str,
        shard_name: &str,
        node_name: &str,
    ) -> Result<NodeStatus, Box<dyn Error + Send + Sync>> {
        info!(
            "get_node: index_name={}, shard_name={}, node_name={}",
            index_name, shard_name, node_name
        );

        let key = format!("{}/{}/{}/{}", &self.root, index_name, shard_name, node_name);
        let get_response = match self.client.get(key, None).await {
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
                format!(
                    "key does not fount: {}",
                    format!("{}/{}/{}/{}", &self.root, index_name, shard_name, node_name)
                ),
            ))),
        }
    }

    async fn get_nodes(
        &mut self,
        index_name: &str,
    ) -> Result<Vec<NodeStatus>, Box<dyn Error + Send + Sync>> {
        info!("get_nodes: cluster={}", index_name);

        // let mut interval = time::interval(Duration::from_secs(3));
        // loop {
        //     interval.tick().await;
        //
        //     let key = format!("{}/{}/", &self.root, cluster);
        //     let get_response = match self
        //         .client
        //         .get(key, Some(GetOptions::new().with_prefix()))
        //         .await
        //     {
        //         Ok(get_response) => get_response,
        //         Err(e) => {
        //             error!("{:?}", e);
        //             continue;
        //         }
        //     };
        //
        //     for kv in get_response.kvs() {
        //         info!("{}:{}", kv.key_str().unwrap(), kv.value_str().unwrap());
        //     }
        // }

        // let (mut watcher, mut stream) = self.client.watch(key, None).await.unwrap();
        //
        // while let Some(resp) = stream.message().await.unwrap() {
        //     info!("receive watch response");
        //
        //     if resp.canceled() {
        //         info!("watch canceled");
        //         break;
        //     }
        //
        //     for event in resp.events() {
        //         info!("event type: {:?}", event.event_type());
        //         if let Some(kv) = event.kv() {
        //             info!("kv: {{{}: {}}}", kv.key_str().unwrap(), kv.value_str().unwrap());
        //         }
        //
        //         if EventType::Delete == event.event_type() {
        //             watcher.cancel().await?;
        //         }
        //     }
        // }

        let r = vec![];

        Ok(r)
    }
}
