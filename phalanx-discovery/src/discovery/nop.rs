use std::error::Error;

use async_trait::async_trait;

use phalanx_proto::phalanx::NodeDetails;

use crate::discovery::Discovery;

pub const TYPE: &str = "nop";

pub struct Nop {}

impl Nop {
    pub fn new() -> Nop {
        Nop {}
    }
}

#[async_trait]
impl Discovery for Nop {
    fn get_type(&self) -> &str {
        TYPE
    }

    async fn get_node(
        &mut self,
        _index_name: &str,
        _shard_name: &str,
        _node_name: &str,
    ) -> Result<Option<NodeDetails>, Box<dyn Error + Send + Sync>> {
        Ok(None)
    }

    async fn set_node(
        &mut self,
        _index_name: &str,
        _shard_name: &str,
        _node_name: &str,
        _node_details: NodeDetails,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        Ok(())
    }

    async fn delete_node(
        &mut self,
        _index_name: &str,
        _shard_name: &str,
        _node_name: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        Ok(())
    }

    async fn watch_cluster(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        Ok(())
    }

    async fn unwatch_cluster(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        Ok(())
    }

    async fn watch_role(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        Ok(())
    }

    async fn unwatch_role(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        Ok(())
    }

    async fn start_health_check(
        &mut self,
        _interval: u64,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        Ok(())
    }

    async fn stop_health_check(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        Ok(())
    }
}
