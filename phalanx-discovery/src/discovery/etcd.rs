use std::convert::TryFrom;
use std::error::Error;
use std::io::{Error as IOError, ErrorKind};
use std::time::Duration;

use async_std::task::block_on;
use async_trait::async_trait;
use etcd_client::{Client, GetOptions};
use etcd_client::EventType;
use log::*;
use serde::Serialize;
use tokio::time;

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

        let key = format!("{}/{}/{}/{}", &self.root, cluster, shard, node);
        let value = serde_json::to_string(&node_status).unwrap();

        let put_response = match self.client.put(key, value, None).await {
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

        let key = format!("{}/{}/{}/{}", &self.root, cluster, shard, node);
        let delete_response = match self.client.delete(key, None).await {
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

        let key = format!("{}/{}/{}/{}", &self.root, cluster, shard, node);
        let get_response = match self.client.get(key, None).await {
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

    async fn update_cluster(&mut self, cluster: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        info!("update_cluster: cluster={}", cluster);

        let mut interval = time::interval(Duration::from_secs(3));
        loop {
            interval.tick().await;

            let key = format!("{}/{}/", &self.root, cluster);
            let get_response = match self.client.get(key, Some(GetOptions::new().with_prefix())).await {
                Ok(get_response) => {
                    get_response
                }
                Err(e) => {
                    error!("{:?}", e);
                    continue;
                },
            };

            for kv in get_response.kvs() {
                info!("{}:{}", kv.key_str().unwrap(), kv.value_str().unwrap());
            }
        }

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

        Ok(())
    }
}
