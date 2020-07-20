use std::error::Error;

use async_trait::async_trait;

use crate::discovery::{Discovery, NodeStatus};

pub const DISCOVERY_TYPE: &str = "null";

pub struct Null {}

impl Null {
    pub fn new() -> Null {
        Null {}
    }
}

#[async_trait]
impl Discovery for Null {
    fn get_type(&self) -> &str {
        DISCOVERY_TYPE
    }

    async fn set_node(
        &mut self,
        _cluster: &str,
        _shard: &str,
        _node: &str,
        _node_status: NodeStatus,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        unimplemented!()
    }

    async fn delete_node(
        &mut self,
        _cluster: &str,
        _shard: &str,
        _node: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        unimplemented!()
    }

    async fn get_node(
        &mut self,
        _cluster: &str,
        _shard: &str,
        _node: &str,
    ) -> Result<NodeStatus, Box<dyn Error + Send + Sync>> {
        unimplemented!()
    }

    async fn get_nodes(
        &mut self,
        _cluster: &str,
    ) -> Result<Vec<NodeStatus>, Box<dyn Error + Send + Sync>> {
        unimplemented!()
    }
}
