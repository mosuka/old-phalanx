use async_trait::async_trait;
use std::error::Error;

use crate::storage::Storage;

pub const TYPE: &str = "null";

pub struct Nop {}

impl Nop {
    pub fn new() -> Nop {
        Nop {}
    }
}

#[async_trait]
impl Storage for Nop {
    fn get_type(&self) -> &str {
        TYPE
    }

    async fn exist(
        &self,
        _index_name: &str,
        _shard_name: &str,
    ) -> Result<bool, Box<dyn Error + Send + Sync>> {
        Ok(false)
    }

    async fn pull_index(
        &self,
        _index_name: &str,
        _shard_name: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        Ok(())
    }

    async fn push_index(
        &self,
        _index_name: &str,
        _shard_name: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        Ok(())
    }

    // async fn sync_index(&mut self, _interval: u64) -> Result<(), Box<dyn Error + Send + Sync>> {
    //     Ok(())
    // }

    // async fn unsync_index(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
    //     Ok(())
    // }
}
