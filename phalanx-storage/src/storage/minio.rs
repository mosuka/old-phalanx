use std::convert::TryFrom;
use std::error::Error;
use std::fs;
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
use tantivy::Index;
use tokio::fs::{create_dir_all, File};
use tokio::io;
use tokio::prelude::*;
use walkdir::WalkDir;

use crate::storage::Storage;

pub const STORAGE_TYPE: &str = "minio";

pub struct Minio {
    pub client: S3Client,
    bucket: String,
    index_dir: String,
}

impl Minio {
    pub fn new(
        access_key: &str,
        secret_key: &str,
        endpoint: &str,
        bucket: &str,
        index_dir: &str,
    ) -> Minio {
        let credentials =
            StaticProvider::new_minimal(access_key.to_string(), secret_key.to_string());

        let region = Region::Custom {
            name: "us-east-2".to_string(),
            endpoint: endpoint.to_string(),
        };

        let client = S3Client::new_with(HttpClient::new().unwrap(), credentials, region);

        Minio {
            client,
            bucket: bucket.to_string(),
            index_dir: index_dir.to_string(),
        }
    }

    async fn list_segments(
        &self,
        key: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        // Retrieves a segment file list of an index stored in object storage
        let keys = match self
            .client
            .get_object(GetObjectRequest {
                bucket: self.bucket.clone(),
                key: format!("{}/{}", key, ".managed.json"),
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
                    // exclude meta.json
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
        key: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        // list
        let keys = match self
            .client
            .list_objects(ListObjectsRequest {
                bucket: self.bucket.clone(),
                prefix: Some(key.to_string()),
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

        // put
        match self
            .client
            .put_object(PutObjectRequest {
                bucket: self.bucket.clone(),
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
        key: &str,
        path: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("pull: {} to {}", key, path);

        // get object
        let output = match self
            .client
            .get_object(GetObjectRequest {
                bucket: self.bucket.clone(),
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

    async fn delete(&self, key: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("delete: {}", key);

        match self
            .client
            .delete_object(DeleteObjectRequest {
                bucket: self.bucket.clone(),
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

#[async_trait]
impl Storage for Minio {
    fn get_type(&self) -> &str {
        STORAGE_TYPE
    }

    async fn pull_index(
        &self,
        cluster: &str,
        shard: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let key = format!("{}/{}", cluster, shard);

        // list segments in object storage
        let mut object_names = match self.list_segments(&key).await {
            Ok(segments) => segments,
            Err(e) => return Err(e),
        };
        // add index files
        object_names.push(String::from(".managed.json"));
        object_names.push(String::from("meta.json"));

        // pull objects from object storage
        for object_name in object_names.clone() {
            let objct_key = format!("{}/{}", &key, &object_name);
            let file_path = String::from(
                Path::new(&self.index_dir)
                    .join(&object_name)
                    .to_str()
                    .unwrap(),
            );

            match self.pull(&objct_key, &file_path).await {
                Ok(_) => (),
                Err(e) => {
                    return Err(e);
                }
            };
        }

        // list all files in a local index
        let mut file_names = Vec::new();
        for entry in WalkDir::new(&self.index_dir)
            .follow_links(true)
            .into_iter()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_type().is_file())
        {
            let file_name = entry.file_name().to_str().unwrap();
            // exclude lock files
            if !file_name.ends_with(".lock") {
                file_names.push(String::from(file_name));
            }
        }

        // remove unnecessary index files from local index
        for file_name in file_names {
            if !object_names.contains(&file_name) {
                let file_path = String::from(
                    Path::new(&self.index_dir)
                        .join(&file_name)
                        .to_str()
                        .unwrap(),
                );
                match fs::remove_file(&file_path) {
                    Ok(()) => info!("delete: {}", &file_path),
                    Err(e) => {
                        return Err(Box::try_from(e).unwrap());
                    }
                };
            }
        }

        Ok(())
    }

    async fn push_index(
        &self,
        cluster: &str,
        shard: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let key = format!("{}/{}", cluster, shard);

        // list segments in local index directory
        let index = Index::open_in_dir(&self.index_dir).unwrap();
        let mut index_files = Vec::new();
        for segment in index.searchable_segments().unwrap() {
            for segment_file_path in segment.meta().list_files() {
                let segment_file = String::from(segment_file_path.to_str().unwrap());
                index_files.push(segment_file);
            }
        }
        // add index files
        index_files.push(String::from(".managed.json"));
        index_files.push(String::from("meta.json"));

        // push files to object storage
        for index_file in index_files.clone() {
            let file_path = String::from(
                Path::new(&self.index_dir)
                    .join(&index_file)
                    .to_str()
                    .unwrap(),
            );
            let object_key = format!("{}/{}", &key, &index_file);

            match self.push(&file_path, &object_key).await {
                Ok(_) => (),
                Err(e) => {
                    return Err(e);
                }
            };
        }

        // list all objects under the specified key
        let object_names = match self.list(&key).await {
            Ok(keys) => keys,
            Err(e) => {
                return Err(e);
            }
        };

        // remove unnecessary objects from object storage
        for object_name in object_names {
            if !index_files.contains(&object_name) {
                // meta.json -> cluster1/shard1/meta.json
                let object_key = String::from(Path::new(&key).join(&object_name).to_str().unwrap());

                match self.delete(&object_key).await {
                    Ok(_) => (),
                    Err(e) => {
                        return Err(e);
                    }
                };
            }
        }

        Ok(())
    }
}
