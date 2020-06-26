pub mod minio;
pub mod null;

// use crate::storage::minio::Minio;
use async_trait::async_trait;

#[async_trait]
pub trait Storage: Send + Sync + 'static {
    fn get_type(&self) -> &str;

    async fn segments(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>>;

    async fn list(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>>;

    async fn push(
        &self,
        path: &str,
        bucket: &str,
        key: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    async fn pull(
        &self,
        bucket: &str,
        key: &str,
        path: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    async fn delete(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

// pub fn get_storage(storage_type: &str) -> Box<dyn Storage> {
//     let storage = match storage_type {
//         "minio" => Box::new(Minio::new("minioadmin", "minioadmin", "http://localhost:9000")),
//         _ => panic!("unsupported storage type: {}", storage_type),
//     };
//
//     storage
// }
