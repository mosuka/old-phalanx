use async_trait::async_trait;
use std::error::Error;

use crate::storage::Storage;

pub const STORAGE_TYPE: &str = "null";

pub struct Null {}

impl Null {
    pub fn new() -> Null {
        Null {}
    }
}

#[async_trait]
impl Storage for Null {
    fn get_type(&self) -> &str {
        STORAGE_TYPE
    }

    async fn pull_index(
        &self,
        _cluster: &str,
        _shard: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        unimplemented!()
    }

    async fn push_index(
        &self,
        _cluster: &str,
        _shard: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        unimplemented!()
    }
}
