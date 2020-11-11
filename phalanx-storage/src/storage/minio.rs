use std::error::Error;
// use std::fs;
// use std::io::ErrorKind;
// use std::path::Path;
// use std::sync::atomic::{AtomicBool, Ordering};
// use std::sync::Arc;

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
// use serde_json::Value;
// use tokio::fs::File;
// use tokio::io;
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
    // bucket: String,
    // index_dir: String,
    // stop_index_syncer: Arc<AtomicBool>,
    // index_syncer_running: Arc<AtomicBool>,
}

impl Minio {
    pub fn new(
        // access_key: &str,
        // secret_key: &str,
        // endpoint: &str,
        // bucket: &str,
        // index_dir: &str,
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
            // bucket: bucket.to_string(),
            // index_dir: index_dir.to_string(),
            // stop_index_syncer: Arc::new(AtomicBool::new(false)),
            // index_syncer_running: Arc::new(AtomicBool::new(false)),
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

    // async fn pull(
    //     &self,
    //     index_name: &str,
    //     shard_name: &str,
    // ) -> Result<(), Box<dyn Error + Send + Sync>> {
    //     // list objects
    //     let managed_json_key = format!("{}/{}/.managed.json", index_name, shard_name);
    //     let mut object_names = match self
    //         .client
    //         .get_object(GetObjectRequest {
    //             bucket: self.bucket.clone(),
    //             key: managed_json_key,
    //             ..Default::default()
    //         })
    //         .await
    //     {
    //         Ok(output) => {
    //             // read content
    //             let mut content = String::new();
    //             output
    //                 .body
    //                 .unwrap()
    //                 .into_async_read()
    //                 .read_to_string(&mut content)
    //                 .await
    //                 .unwrap();
    //
    //             // parse content
    //             let value: Value = serde_json::from_str(&content).unwrap();
    //
    //             // create object name list
    //             let mut object_names = Vec::new();
    //             for object_name in value.as_array().unwrap() {
    //                 object_names.push(String::from(object_name.as_str().unwrap()));
    //             }
    //             object_names
    //         }
    //         Err(e) => return Err(Box::new(e)),
    //     };
    //     object_names.push(".managed.json".to_string());
    //
    //     // pull objects
    //     for object_name in object_names.clone() {
    //         let object_key = format!("{}/{}/{}", index_name, shard_name, &object_name);
    //         let file_path = String::from(
    //             Path::new(&self.index_dir)
    //                 .join(&object_key)
    //                 .to_str()
    //                 .unwrap(),
    //         );
    //
    //         // get object
    //         match self
    //             .client
    //             .get_object(GetObjectRequest {
    //                 bucket: self.bucket.clone(),
    //                 key: object_key,
    //                 ..Default::default()
    //             })
    //             .await
    //         {
    //             Ok(output) => {
    //                 // copy to file
    //                 let mut data = output.body.unwrap().into_async_read();
    //                 let mut file = File::create(file_path).await.unwrap();
    //                 io::copy(&mut data, &mut file).await.unwrap();
    //             }
    //             Err(e) => return Err(Box::new(e)),
    //         };
    //     }
    //
    //     // list files
    //     let mut file_names = Vec::new();
    //     for entry in WalkDir::new(&self.index_dir)
    //         .follow_links(true)
    //         .into_iter()
    //         .filter_map(|e| e.ok())
    //         .filter(|e| e.file_type().is_file())
    //     {
    //         let file_name = entry.file_name().to_str().unwrap();
    //         // exclude lock files
    //         if !file_name.ends_with(".lock") {
    //             file_names.push(String::from(file_name));
    //         }
    //     }
    //
    //     // remove unnecessary files
    //     for file_name in file_names {
    //         if !object_names.contains(&file_name) {
    //             let file_path = String::from(
    //                 Path::new(&self.index_dir)
    //                     .join(&file_name)
    //                     .to_str()
    //                     .unwrap(),
    //             );
    //             match fs::remove_file(&file_path) {
    //                 Ok(()) => debug!("delete: {}", &file_path),
    //                 Err(e) => {
    //                     return Err(Box::new(e));
    //                 }
    //             };
    //         }
    //     }
    //
    //     Ok(())
    // }

    // async fn push(
    //     &self,
    //     index_name: &str,
    //     shard_name: &str,
    // ) -> Result<(), Box<dyn Error + Send + Sync>> {
    //     // list files
    //     let mut file_names = Vec::new();
    //     for entry in WalkDir::new(&self.index_dir)
    //         .follow_links(true)
    //         .into_iter()
    //         .filter_map(|e| e.ok())
    //         .filter(|e| e.file_type().is_file())
    //     {
    //         let file_name = entry.file_name().to_str().unwrap();
    //         // exclude lock files
    //         if !file_name.ends_with(".lock") {
    //             file_names.push(String::from(file_name));
    //         }
    //     }
    //
    //     // push files
    //     for file_name in file_names.clone() {
    //         let file_path = String::from(
    //             Path::new(&self.index_dir)
    //                 .join(&file_name)
    //                 .to_str()
    //                 .unwrap(),
    //         );
    //
    //         // read file
    //         let mut file = File::open(&file_path).await.unwrap();
    //         let mut data: Vec<u8> = Vec::new();
    //         file.read_to_end(&mut data).await.unwrap();
    //
    //         let object_key = format!("{}/{}/{}", index_name, shard_name, &file_name);
    //
    //         // put object
    //         match self
    //             .client
    //             .put_object(PutObjectRequest {
    //                 bucket: self.bucket.clone(),
    //                 key: object_key,
    //                 body: Some(ByteStream::from(data)),
    //                 ..Default::default()
    //             })
    //             .await
    //         {
    //             Ok(_output) => (),
    //             Err(e) => return Err(Box::new(e)),
    //         };
    //     }
    //
    //     let shard_key = format!("{}/{}/", index_name, shard_name);
    //
    //     // list objects
    //     let object_names = match self
    //         .client
    //         .list_objects(ListObjectsRequest {
    //             bucket: self.bucket.clone(),
    //             prefix: Some(shard_key.clone()),
    //             ..Default::default()
    //         })
    //         .await
    //     {
    //         Ok(output) => {
    //             let mut object_names = Vec::new();
    //             match output.contents {
    //                 Some(contents) => {
    //                     for content in contents {
    //                         let k = content.key.unwrap();
    //                         // cluster1/shard1/meta.json -> meta.json
    //                         let p = Path::new(&k).strip_prefix(&shard_key).unwrap();
    //                         object_names.push(String::from(p.to_str().unwrap()));
    //                     }
    //                 }
    //                 None => (),
    //             }
    //
    //             object_names
    //         }
    //         Err(e) => return Err(Box::new(e)),
    //     };
    //
    //     // remove unnecessary objects
    //     for object_name in object_names {
    //         if !file_names.contains(&object_name) {
    //             // e.g. meta.json -> cluster1/shard1/meta.json
    //             let object_key = format!("{}/{}/{}", index_name, shard_name, &object_name);
    //
    //             match self
    //                 .client
    //                 .delete_object(DeleteObjectRequest {
    //                     bucket: self.bucket.clone(),
    //                     key: object_key,
    //                     ..Default::default()
    //                 })
    //                 .await
    //             {
    //                 Ok(_output) => (),
    //                 Err(e) => return Err(Box::new(e)),
    //             };
    //         }
    //     }
    //
    //     Ok(())
    // }
}
