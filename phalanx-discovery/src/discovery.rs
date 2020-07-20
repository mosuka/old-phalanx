pub mod etcd;
pub mod null;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[async_trait]
pub trait Discovery: Send + Sync + 'static {
    fn get_type(&self) -> &str;

    async fn set_node(
        &mut self,
        cluster: &str,
        shard: &str,
        node: &str,
        node_status: NodeStatus,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    async fn delete_node(
        &mut self,
        cluster: &str,
        shard: &str,
        node: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    async fn get_node(
        &mut self,
        cluster: &str,
        shard: &str,
        node: &str,
    ) -> Result<NodeStatus, Box<dyn std::error::Error + Send + Sync>>;

    async fn get_nodes(
        &mut self,
        cluster: &str,
    ) -> Result<Vec<NodeStatus>, Box<dyn std::error::Error + Send + Sync>>;
}

#[derive(Clone, Serialize, Deserialize, Debug)]
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
