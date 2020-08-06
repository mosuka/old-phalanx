pub mod minio;
pub mod nop;

use async_trait::async_trait;

#[async_trait]
pub trait Storage: Send + Sync + 'static {
    fn get_type(&self) -> &str;

    async fn pull_index(
        &self,
        cluster: &str,
        shard: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    async fn push_index(
        &self,
        cluster: &str,
        shard: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}
