use std::collections::hash_map::RandomState;
use std::collections::HashMap;
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

    async fn get_indices(&mut self) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        Ok(Vec::new())
    }

    async fn get_shards(
        &mut self,
        _index_name: &str,
    ) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        Ok(Vec::new())
    }

    async fn get_nodes(
        &mut self,
        _index_name: &str,
        _shard_name: &str,
    ) -> Result<HashMap<String, Option<NodeDetails>>, Box<dyn Error + Send + Sync>> {
        Ok(HashMap::new())
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

    async fn get_primary_node(
        &mut self,
        _index_name: &str,
        _shard_name: &str,
    ) -> Result<Option<String>, Box<dyn Error + Send + Sync>> {
        Ok(None)
    }

    async fn get_replica_nodes(
        &mut self,
        _index_name: &str,
        _shard_name: &str,
    ) -> Result<HashMap<String, Option<NodeDetails>, RandomState>, Box<dyn Error + Send + Sync>>
    {
        Ok(HashMap::new())
    }

    async fn get_candidate_nodes(
        &mut self,
        _index_name: &str,
        _shard_name: &str,
    ) -> Result<HashMap<String, Option<NodeDetails>, RandomState>, Box<dyn Error + Send + Sync>>
    {
        Ok(HashMap::new())
    }

    async fn start_watch(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        Ok(())
    }

    async fn stop_watch(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        Ok(())
    }

    async fn start_healthcheck(
        &mut self,
        _interval: u64,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        Ok(())
    }

    async fn stop_healthcheck(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        Ok(())
    }
}
