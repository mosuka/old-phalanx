pub mod etcd;
pub mod nop;

use async_trait::async_trait;
use prometheus::GaugeVec;

use phalanx_proto::phalanx::NodeDetails;

pub const CLUSTER_PATH: &str = "cluster";
pub const INDEX_META_PATH: &str = "index_meta";

lazy_static! {
    static ref NODE_STATE_GAUGE: GaugeVec = register_gauge_vec!(
        "phalanx_discovery_node_state",
        "Node state.",
        &["index", "shard", "node"]
    )
    .unwrap();
    static ref NODE_ROLE_GAUGE: GaugeVec = register_gauge_vec!(
        "phalanx_discovery_node_role",
        "Node role.",
        &["index", "shard", "node"]
    )
    .unwrap();
}

#[async_trait]
pub trait Discovery: Send + Sync + 'static {
    fn get_type(&self) -> &str;

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

    async fn watch_cluster(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    async fn unwatch_cluster(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    async fn watch_role(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    async fn unwatch_role(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    async fn start_health_check(
        &mut self,
        interval: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    async fn stop_health_check(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    async fn get_index_meta(
        &mut self,
        index_name: &str,
        shard_name: &str,
    ) -> Result<Option<String>, Box<dyn std::error::Error + Send + Sync>>;

    async fn set_index_meta(
        &mut self,
        index_name: &str,
        shard_name: &str,
        index_meta: String,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    async fn delete_index_meta(
        &mut self,
        index_name: &str,
        shard_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    // async fn watch_index_meta(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    //
    // async fn unwatch_index_meta(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}
