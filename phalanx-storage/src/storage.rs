pub mod minio;
pub mod nop;

use async_trait::async_trait;
use dyn_clone::{clone_trait_object, DynClone};

#[async_trait]
pub trait Storage: DynClone + Send + Sync + 'static {
    fn get_type(&self) -> &str;

    async fn exist(&self, key: &str) -> Result<bool, Box<dyn std::error::Error + Send + Sync>>;

    async fn get(
        &self,
        key: &str,
    ) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>>;

    async fn list(
        &self,
        prefix: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>>;

    async fn set(
        &self,
        key: &str,
        content: &[u8],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    async fn delete(&self, key: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    // async fn pull(
    //     &self,
    //     index_name: &str,
    //     shard_name: &str,
    // ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    // async fn push(
    //     &self,
    //     index_name: &str,
    //     shard_name: &str,
    // ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

clone_trait_object!(Storage);

#[derive(Clone)]
pub struct StorageContainer {
    pub storage: Box<dyn Storage>,
}
