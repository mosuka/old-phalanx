use std::collections::HashMap;
use std::error::Error;

use async_trait::async_trait;

use crate::discovery::{Discovery, NodeKey, NodeStatus};

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
        _node_key: NodeKey,
        _node_status: NodeStatus,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        unimplemented!()
    }

    async fn delete_node(
        &mut self,
        _node_key: NodeKey,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        unimplemented!()
    }

    async fn get_node(
        &mut self,
        _node_key: NodeKey,
    ) -> Result<Option<NodeStatus>, Box<dyn Error + Send + Sync>> {
        unimplemented!()
    }

    async fn get_nodes(
        &mut self,
    ) -> Result<HashMap<NodeKey, NodeStatus>, Box<dyn Error + Send + Sync>> {
        unimplemented!()
    }
}
