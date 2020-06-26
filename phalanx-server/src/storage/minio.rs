use std::convert::TryFrom;
use std::io::ErrorKind;
use std::path::Path;

use async_trait::async_trait;
use log::*;
use rusoto_core::credential::StaticProvider;
use rusoto_core::request::HttpClient;
use rusoto_core::{ByteStream, Region};
use rusoto_s3::{
    DeleteObjectRequest, GetObjectRequest, ListObjectsRequest, PutObjectRequest, S3Client, S3,
};
use serde_json::Value;
use tokio::fs::{create_dir_all, File};
use tokio::io;
use tokio::prelude::*;

use crate::storage::Storage;

pub const STORAGE_TYPE: &str = "minio";

pub struct Minio {
    pub(crate) client: S3Client,
}

impl Minio {
    pub fn new(access_key: &str, secret_key: &str, endpoint: &str) -> Minio {
        let credentials =
            StaticProvider::new_minimal(access_key.to_string(), secret_key.to_string());

        let region = Region::Custom {
            name: "us-east-2".to_string(),
            endpoint: endpoint.to_string(),
        };

        let client = S3Client::new_with(HttpClient::new().unwrap(), credentials, region);

        Minio { client }
    }
}

#[async_trait]
impl Storage for Minio {
    fn get_type(&self) -> &str {
        STORAGE_TYPE
    }

    async fn segments(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        let managed_json_path = Path::new(key).join(".managed.json");

        // get segments
        let keys = match self
            .client
            .get_object(GetObjectRequest {
                bucket: String::from(bucket),
                key: String::from(managed_json_path.to_str().unwrap()),
                ..Default::default()
            })
            .await
        {
            Ok(output) => {
                let mut keys = Vec::new();

                let mut content = String::new();
                output
                    .body
                    .unwrap()
                    .into_async_read()
                    .read_to_string(&mut content)
                    .await
                    .unwrap();

                let value: Value = serde_json::from_str(&content).unwrap();
                for v in value.as_array().unwrap() {
                    if v.as_str().unwrap() != "meta.json" {
                        keys.push(String::from(v.as_str().unwrap()));
                    }
                }

                keys
            }
            Err(e) => return Err(Box::try_from(e).unwrap()),
        };

        Ok(keys)
    }

    async fn list(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        // list
        let keys = match self
            .client
            .list_objects(ListObjectsRequest {
                bucket: String::from(bucket),
                prefix: Some(String::from(key)),
                ..Default::default()
            })
            .await
        {
            Ok(output) => {
                let mut keys = Vec::new();

                for content in output.contents.unwrap() {
                    let k = content.key.unwrap();
                    // cluster1/shard1/meta.json -> meta.json
                    let p = Path::new(&k).strip_prefix(key).unwrap();
                    keys.push(String::from(p.to_str().unwrap()));
                }

                keys
            }
            Err(e) => return Err(Box::try_from(e).unwrap()),
        };

        Ok(keys)
    }

    async fn push(
        &self,
        path: &str,
        bucket: &str,
        key: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("push: {} to {}", path, key);

        let file_path = Path::new(path);
        if !file_path.exists() {
            debug!("file does not exist: {}", path);
            return Ok(());
        }

        // read file
        let mut file = File::open(path).await.unwrap();
        let mut data: Vec<u8> = Vec::new();
        file.read_to_end(&mut data).await.unwrap();

        // delete
        match self
            .client
            .delete_object(DeleteObjectRequest {
                bucket: bucket.to_string(),
                key: key.to_string(),
                ..Default::default()
            })
            .await
        {
            Ok(_output) => (),
            Err(e) => return Err(Box::try_from(e).unwrap()),
        };

        // put
        match self
            .client
            .put_object(PutObjectRequest {
                bucket: bucket.to_string(),
                key: key.to_string(),
                body: Some(ByteStream::from(data)),
                ..Default::default()
            })
            .await
        {
            Ok(_output) => (),
            Err(e) => return Err(Box::try_from(e).unwrap()),
        };

        Ok(())
    }

    async fn pull(
        &self,
        bucket: &str,
        key: &str,
        path: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("pull: {} to {}", key, path);

        // get object
        let output = match self
            .client
            .get_object(GetObjectRequest {
                bucket: bucket.to_string(),
                key: key.to_string(),
                ..Default::default()
            })
            .await
        {
            Ok(output) => output,
            Err(e) => return Err(Box::try_from(e).unwrap()),
        };

        // create directory
        let parent = Path::new(path).parent().unwrap();
        match create_dir_all(parent).await {
            Ok(_) => debug!("create directory: {}", parent.to_str().unwrap()),
            Err(e) => {
                if e.kind() == ErrorKind::AlreadyExists {
                    debug!("already exists: {}", parent.to_str().unwrap())
                } else {
                    return Err(Box::try_from(e).unwrap());
                }
            }
        };

        // download file
        let mut data = output.body.unwrap().into_async_read();
        let mut file = File::create(path).await.unwrap();
        io::copy(&mut data, &mut file).await.unwrap();

        Ok(())
    }

    async fn delete(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("delete: {}", key);

        match self
            .client
            .delete_object(DeleteObjectRequest {
                bucket: String::from(bucket),
                key: String::from(key),
                ..Default::default()
            })
            .await
        {
            Ok(_output) => (),
            Err(e) => return Err(Box::try_from(e).unwrap()),
        };

        Ok(())
    }
}
