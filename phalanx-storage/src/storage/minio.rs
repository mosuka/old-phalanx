use std::error::Error;

use async_std::task::block_on;
use async_trait::async_trait;
use log::*;
use rusoto_core::credential::StaticProvider;
use rusoto_core::request::HttpClient;
use rusoto_core::{ByteStream, Region};
use rusoto_s3::{
    CreateBucketConfiguration, CreateBucketRequest, DeleteObjectRequest, GetObjectRequest,
    ListObjectsRequest, PutObjectRequest, S3Client, S3,
};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;

use crate::storage::Storage;

pub const TYPE: &str = "minio";

#[derive(Clone, Serialize, Deserialize)]
pub struct MinioConfig {
    pub access_key: String,
    pub secret_key: String,
    pub endpoint: String,
    pub bucket: String,
}

#[derive(Clone)]
pub struct Minio {
    config: MinioConfig,
    client: S3Client,
}

impl Minio {
    pub fn new(
        config: MinioConfig,
    ) -> Minio {
        let credentials =
            StaticProvider::new_minimal(config.access_key.clone(), config.secret_key.clone());

        let region = Region::Custom {
            name: "us-east-2".to_string(),
            endpoint: config.endpoint.clone(),
        };

        let client = S3Client::new_with(HttpClient::new().unwrap(), credentials, region);

        let future = client.create_bucket(CreateBucketRequest {
            bucket: config.bucket.clone(),
            create_bucket_configuration: Some(CreateBucketConfiguration {
                location_constraint: Some("us-east-2".to_string()),
            }),
            ..Default::default()
        });
        match block_on(future) {
            Ok(_output) => (),
            Err(e) => {
                warn!("failed to create bucket: error = {:?}", e);
            }
        };

        Minio {
            config,
            client,
        }
    }
}

#[async_trait]
impl Storage for Minio {
    fn get_type(&self) -> &str {
        TYPE
    }

    async fn exist(&self, key: &str) -> Result<bool, Box<dyn Error + Send + Sync>> {
        match self
            .client
            .get_object(GetObjectRequest {
                bucket: self.config.bucket.clone(),
                key: key.to_string(),
                ..Default::default()
            })
            .await
        {
            Ok(output) => match output.body {
                Some(_sb) => Ok(true),
                None => {
                    debug!("empty body");
                    Ok(false)
                }
            },
            Err(e) => {
                debug!("failed to get meta.json: error={:?}", e);
                Ok(false)
            }
        }
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Box<dyn Error + Send + Sync>> {
        match self
            .client
            .get_object(GetObjectRequest {
                bucket: self.config.bucket.clone(),
                key: key.to_string(),
                ..Default::default()
            })
            .await
        {
            Ok(output) => {
                let mut content = Vec::new();
                match output.body {
                    Some(sb) => match sb.into_async_read().read_to_end(&mut content).await {
                        Ok(_s) => Ok(Some(content)),
                        Err(e) => {
                            error!("failed to read body: error={:?}", e);
                            Ok(None)
                        }
                    },
                    None => {
                        error!("empty body");
                        Ok(None)
                    }
                }
            }
            Err(e) => {
                error!("failed to get meta.json: error={:?}", e);
                Err(Box::new(e))
            }
        }
    }

    async fn list(&self, prefix: &str) -> Result<Vec<String>, Box<dyn Error + Send + Sync>> {
        match self
            .client
            .list_objects(ListObjectsRequest {
                bucket: self.config.bucket.clone(),
                prefix: Some(prefix.to_string()),
                ..Default::default()
            })
            .await
        {
            Ok(output) => {
                let mut keys = Vec::new();
                match output.contents {
                    Some(contents) => {
                        for content in contents {
                            keys.push(content.key.unwrap());
                        }
                    }
                    None => (),
                }

                Ok(keys)
            }
            Err(e) => Err(Box::new(e)),
        }
    }

    async fn set(&self, key: &str, content: &[u8]) -> Result<(), Box<dyn Error + Send + Sync>> {
        match self
            .client
            .put_object(PutObjectRequest {
                bucket: self.config.bucket.clone(),
                key: key.to_string(),
                body: Some(ByteStream::from(content.to_vec())),
                ..Default::default()
            })
            .await
        {
            Ok(_output) => Ok(()),
            Err(e) => Err(Box::new(e)),
        }
    }

    async fn delete(&self, key: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        match self
            .client
            .delete_object(DeleteObjectRequest {
                bucket: self.config.bucket.clone(),
                key: key.to_string(),
                ..Default::default()
            })
            .await
        {
            Ok(_output) => Ok(()),
            Err(e) => Err(Box::new(e)),
        }
    }
}
