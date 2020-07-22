pub mod etcd;
pub mod null;

use std::collections::HashMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[async_trait]
pub trait Discovery: Send + Sync + 'static {
    fn get_type(&self) -> &str;

    async fn set_node(
        &mut self,
        node_key: NodeKey,
        node_status: NodeStatus,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    async fn delete_node(
        &mut self,
        node_key: NodeKey,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    async fn get_node(
        &mut self,
        node_key: NodeKey,
    ) -> Result<Option<NodeStatus>, Box<dyn std::error::Error + Send + Sync>>;

    async fn get_nodes(
        &mut self,
    ) -> Result<HashMap<NodeKey, NodeStatus>, Box<dyn std::error::Error + Send + Sync>>;
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Debug)]
pub struct NodeKey {
    pub index_name: String,
    pub shard_name: String,
    pub node_name: String,
}

#[derive(Clone, Serialize, Deserialize, PartialOrd, PartialEq, Debug)]
pub enum State {
    Active,
    Inactive,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct NodeStatus {
    pub state: State,
    pub primary: bool,
    pub address: String,
}
