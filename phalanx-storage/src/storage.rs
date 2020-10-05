pub mod minio;
pub mod nop;

use async_trait::async_trait;

#[async_trait]
pub trait Storage: Send + Sync + 'static {
    fn get_type(&self) -> &str;

    async fn exist(
        &self,
        index_name: &str,
        shard_name: &str,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>>;

    async fn pull_index(
        &self,
        index_name: &str,
        shard_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    async fn push_index(
        &self,
        index_name: &str,
        shard_name: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    // async fn sync_index(
    //     &mut self,
    //     interval: u64,
    // ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    // async fn unsync_index(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}
