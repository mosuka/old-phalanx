use async_trait::async_trait;
use std::error::Error;

use crate::storage::Storage;

pub const TYPE: &str = "null";

#[derive(Clone)]
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

    async fn exist(&self, _key: &str) -> Result<bool, Box<dyn Error + Send + Sync>> {
        Ok(false)
    }

    async fn get(&self, _key: &str) -> Result<Option<Vec<u8>>, Box<dyn Error + Send + Sync>> {
        Ok(None)
    }

    async fn list(&self, _prefix: &str) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        Ok(Vec::new())
    }

    async fn set(&self, _key: &str, _content: &[u8]) -> Result<(), Box<dyn Error + Send + Sync>> {
        Ok(())
    }

    async fn delete(&self, _key: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        Ok(())
    }

    // async fn pull(
    //     &self,
    //     _index_name: &str,
    //     _shard_name: &str,
    // ) -> Result<(), Box<dyn Error + Send + Sync>> {
    //     Ok(())
    // }

    // async fn push(
    //     &self,
    //     _index_name: &str,
    //     _shard_name: &str,
    // ) -> Result<(), Box<dyn Error + Send + Sync>> {
    //     Ok(())
    // }
}
