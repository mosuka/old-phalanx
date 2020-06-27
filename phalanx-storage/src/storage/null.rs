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

    async fn segments(
        &self,
        _bucket: &str,
        _key: &str,
    ) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        unimplemented!()
    }

    async fn list(
        &self,
        _bucket: &str,
        _key: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        unimplemented!()
    }

    async fn push(
        &self,
        _path: &str,
        _bucket: &str,
        _key: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        unimplemented!()
    }

    async fn pull(
        &self,
        _bucket: &str,
        _key: &str,
        _path: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        unimplemented!()
    }

    async fn delete(
        &self,
        _bucket: &str,
        _key: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        unimplemented!()
    }
}
