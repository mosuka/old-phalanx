pub mod etcd;
pub mod nop;

use std::collections::HashMap;

use async_trait::async_trait;

use phalanx_proto::phalanx::NodeDetails;

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
    ) -> Result<HashMap<String, Option<NodeDetails>>, Box<dyn std::error::Error + Send + Sync>>;

    async fn get_node(
        &mut self,
        index_name: &str,
        shard_name: &str,
        node_name: &str,
    ) -> Result<Option<NodeDetails>, Box<dyn std::error::Error + Send + Sync>>;

    async fn set_node(
        &mut self,
        index_name: &str,
        shard_name: &str,
        node_name: &str,
        node_details: NodeDetails,
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
    ) -> Result<HashMap<String, Option<NodeDetails>>, Box<dyn std::error::Error + Send + Sync>>;

    async fn get_candidate_nodes(
        &mut self,
        index_name: &str,
        shard_name: &str,
    ) -> Result<HashMap<String, Option<NodeDetails>>, Box<dyn std::error::Error + Send + Sync>>;

    async fn start_watch(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    async fn stop_watch(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    async fn start_healthcheck(
        &mut self,
        interval: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    async fn stop_healthcheck(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}
