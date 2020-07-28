pub mod etcd;
pub mod null;

use std::collections::HashMap;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[async_trait]
pub trait Discovery: Send + Sync + 'static {
    fn get_type(&self) -> &str;

    async fn get_indices(
        &mut self,
    ) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>>;

    async fn get_shards(
        &mut self,
        index_name: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>>;

    async fn get_nodes(
        &mut self,
        index_name: &str,
        shard_name: &str,
    ) -> Result<HashMap<String, Option<NodeStatus>>, Box<dyn std::error::Error + Send + Sync>>;

    async fn get_node(
        &mut self,
        index_name: &str,
        shard_name: &str,
        node_name: &str,
    ) -> Result<Option<NodeStatus>, Box<dyn std::error::Error + Send + Sync>>;

    async fn set_node(
        &mut self,
        index_name: &str,
        shard_name: &str,
        node_name: &str,
        node_status: NodeStatus,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    async fn delete_node(
        &mut self,
        index_name: &str,
        shard_name: &str,
        node_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    async fn get_primary_node(
        &mut self,
        index_name: &str,
        shard_name: &str,
    ) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>>;

    async fn get_replica_nodes(
        &mut self,
        index_name: &str,
        shard_name: &str,
    ) -> Result<HashMap<String, Option<NodeStatus>>, Box<dyn std::error::Error + Send + Sync>>;

    async fn get_candidate_nodes(
        &mut self,
        index_name: &str,
        shard_name: &str,
    ) -> Result<HashMap<String, Option<NodeStatus>>, Box<dyn std::error::Error + Send + Sync>>;
}

#[derive(Clone, Serialize, Deserialize, PartialOrd, PartialEq, Debug)]
pub enum State {
    Ready,
    NotReady,
    Disconnected,
}

#[derive(Clone, Serialize, Deserialize, PartialOrd, PartialEq, Debug)]
pub enum Role {
    Candidate,
    Primary,
    Replica,
}

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct NodeStatus {
    pub state: State,
    pub role: Role,
    pub address: String,
}
