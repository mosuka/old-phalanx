use std::collections::HashMap;
use std::error::Error;

use async_trait::async_trait;

use crate::discovery::{Discovery, NodeStatus};
use std::collections::hash_map::RandomState;

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

    async fn get_indices(&mut self) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        unimplemented!()
    }

    async fn get_shards(
        &mut self,
        _index_name: &str,
    ) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        unimplemented!()
    }

    async fn get_nodes(
        &mut self,
        _index_name: &str,
        _shard_name: &str,
    ) -> Result<HashMap<String, Option<NodeStatus>>, Box<dyn Error + Send + Sync>> {
        unimplemented!()
    }

    async fn get_node(
        &mut self,
        _index_name: &str,
        _shard_name: &str,
        _node_name: &str,
    ) -> Result<Option<NodeStatus>, Box<dyn Error + Send + Sync>> {
        unimplemented!()
    }

    async fn set_node(
        &mut self,
        _index_name: &str,
        _shard_name: &str,
        _node_name: &str,
        _node_status: NodeStatus,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        unimplemented!()
    }

    async fn delete_node(
        &mut self,
        _index_name: &str,
        _shard_name: &str,
        _node_name: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        unimplemented!()
    }

    async fn get_primary_node(
        &mut self,
        _index_name: &str,
        _shard_name: &str,
    ) -> Result<Option<String>, Box<dyn Error + Send + Sync>> {
        unimplemented!()
    }

    async fn get_replica_nodes(
        &mut self,
        _index_name: &str,
        _shard_name: &str,
    ) -> Result<HashMap<String, Option<NodeStatus>, RandomState>, Box<dyn Error + Send + Sync>>
    {
        unimplemented!()
    }

    async fn get_candidate_nodes(
        &mut self,
        _index_name: &str,
        _shard_name: &str,
    ) -> Result<HashMap<String, Option<NodeStatus>, RandomState>, Box<dyn Error + Send + Sync>>
    {
        unimplemented!()
    }
}
