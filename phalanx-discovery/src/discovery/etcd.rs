use std::error::Error;
use std::fs::File;
use std::io::{Error as IOError, ErrorKind, Read};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_std::task::block_on;
use async_trait::async_trait;
use crossbeam::channel::Sender;
use etcd_rs::{Client, ClientConfig, DeleteRequest, EventType, KeyRange, PutRequest, RangeRequest};
use log::*;
use serde::{Deserialize, Serialize};
use tokio::stream::StreamExt;
use tonic::transport::{Certificate, ClientTlsConfig, Identity};

use crate::discovery::{Discovery, Event, EventType as DEventType, KeyValuePair};

pub const TYPE: &str = "etcd";

#[derive(Clone, Serialize, Deserialize)]
pub struct EtcdConfig {
    pub endpoints: Vec<String>,
    pub root: Option<String>,
    pub auth: Option<(String, String)>,
    pub tls_ca_path: Option<String>,
    pub tls_cert_path: Option<String>,
    pub tls_key_path: Option<String>,
}

#[derive(Clone)]
pub struct Etcd {
    config: EtcdConfig,
    client: Client,
    watching: Arc<AtomicBool>,
}

impl Etcd {
    pub fn new(config: EtcdConfig) -> Etcd {
        let tls = if config.tls_ca_path != None
            && config.tls_cert_path != None
            && config.tls_key_path != None
        {
            debug!("configure TLS");
            let mut ca: Vec<u8> = Vec::new();
            let mut cert: Vec<u8> = Vec::new();
            let mut key: Vec<u8> = Vec::new();

            File::open(Path::new("ca.pem"))
                .unwrap()
                .read_to_end(&mut ca)
                .unwrap();
            File::open(Path::new("cert.pem"))
                .unwrap()
                .read_to_end(&mut cert)
                .unwrap();
            File::open(Path::new("key.pem"))
                .unwrap()
                .read_to_end(&mut key)
                .unwrap();

            let tls = ClientTlsConfig::new();
            let tls = tls.ca_certificate(Certificate::from_pem(ca));
            let tls = tls.identity(Identity::from_pem(cert, key));
            Some(tls)
        } else {
            None
        };

        let client = match block_on(Client::connect(ClientConfig {
            endpoints: config.endpoints.clone(),
            auth: config.auth.clone(),
            tls,
        })) {
            Ok(client) => client,
            Err(err) => {
                error!("failed to connect etcd: error = {:?}", err);
                panic!();
            }
        };

        Etcd {
            config,
            client,
            watching: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[async_trait]
impl Discovery for Etcd {
    fn get_type(&self) -> &str {
        TYPE
    }

    fn export_config_json(&self) -> Result<String, IOError> {
        match serde_json::to_string(&self.config) {
            Ok(json) => {
                debug!("config {}", json);
                Ok(json)
            }
            Err(err) => {
                let msg = format!("failed to serialize config: error={:?}", err);
                error!("{}", msg);
                Err(IOError::new(ErrorKind::Other, msg))
            }
        }
    }

    async fn get(&mut self, key: &str) -> Result<Option<String>, Box<dyn Error + Send + Sync>> {
        debug!("get: key={}", key);

        let root = self
            .config
            .root
            .as_ref()
            .unwrap_or(&String::from(""))
            .to_string();
        let actual_key = format!("{}{}", &root, key);

        debug!("get: actual_key={}", &actual_key);
        match self
            .client
            .kv()
            .range(RangeRequest::new(KeyRange::key(actual_key.as_str())))
            .await
        {
            Ok(mut range_response) => match range_response.take_kvs().first() {
                Some(kv) => Ok(Some(kv.value_str().to_string())),
                None => {
                    debug!("not found: actual_key={}", &actual_key);
                    Ok(None)
                }
            },
            Err(err) => {
                let msg = format!("failed to get: actual_key={}, error={:?}", &actual_key, err);
                error!("{}", msg);
                Err(Box::new(err))
            }
        }
    }

    async fn list(
        &mut self,
        prefix: &str,
    ) -> Result<Vec<KeyValuePair>, Box<dyn Error + Send + Sync>> {
        debug!("list: prefix={}", prefix);

        let root = self
            .config
            .root
            .as_ref()
            .unwrap_or(&String::from(""))
            .to_string();
        let actual_prefix = format!("{}{}", &root, prefix);

        debug!("list: actual_prefix={}", &actual_prefix);
        match self
            .client
            .kv()
            .range(RangeRequest::new(KeyRange::prefix(actual_prefix.as_str())))
            .await
        {
            Ok(mut range_response) => {
                let mut kvs = Vec::new();
                for kv in range_response.take_kvs().iter() {
                    let kvp = KeyValuePair {
                        key: String::from(&kv.key_str()[root.clone().len()..]), // remove root
                        value: Some(kv.value_str().to_string()),
                    };
                    kvs.push(kvp);
                }
                if kvs.len() <= 0 {
                    debug!("not found: actual_prefix={}", &actual_prefix);
                }
                Ok(kvs)
            }
            Err(err) => {
                let msg = format!(
                    "failed to list: actual_prefix={}, error={:?}",
                    &actual_prefix, err
                );
                error!("{}", msg);
                Err(Box::new(err))
            }
        }
    }

    async fn put(&mut self, key: &str, value: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        debug!("put: key={}", key);

        let root = self
            .config
            .root
            .as_ref()
            .unwrap_or(&String::from(""))
            .to_string();
        let actual_key = format!("{}{}", &root, key);

        debug!("list: actual_key={}", &actual_key);
        match self
            .client
            .kv()
            .put(PutRequest::new(actual_key.as_str(), value))
            .await
        {
            Ok(_put_response) => Ok(()),
            Err(err) => {
                let msg = format!("failed to put: actual_key={}, error={:?}", &actual_key, err);
                error!("{}", msg);
                Err(Box::new(err))
            }
        }
    }

    async fn delete(&mut self, key: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
        debug!("delete: key={}", key);

        let root = self
            .config
            .root
            .as_ref()
            .unwrap_or(&String::from(""))
            .to_string();
        let actual_key = format!("{}{}", &root, key);

        debug!("list: actual_key={}", &actual_key);
        match self
            .client
            .kv()
            .delete(DeleteRequest::new(KeyRange::key(actual_key.as_str())))
            .await
        {
            Ok(_delete_response) => Ok(()),
            Err(err) => {
                let msg = format!(
                    "failed to delete: actual_key={}, error={:?}",
                    &actual_key, err
                );
                error!("{}", msg);
                Err(Box::new(err))
            }
        }
    }

    async fn watch(
        &mut self,
        sender: Sender<Event>,
        key: &str,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        debug!("watch: key={}", key);

        if self.watching.load(Ordering::Relaxed) {
            let msg = "watcher is already running";
            warn!("{}", msg);
            return Err(Box::new(IOError::new(ErrorKind::Other, msg)));
        }

        let client = self.client.clone();
        let watching = self.watching.clone();
        let root = self
            .config
            .root
            .as_ref()
            .unwrap_or(&String::from(""))
            .to_string();
        let actual_key = format!("{}{}", &root, key);

        tokio::spawn(async move {
            info!("start watch thread");
            watching.store(true, Ordering::Relaxed);

            let sender = sender.clone();
            let client = client.clone();

            debug!("watch: actual_key={}", &actual_key);
            let mut inbound = client.watch(KeyRange::prefix(actual_key.as_str())).await;
            while let Some(result) = inbound.next().await {
                match result {
                    Ok(mut watch_response) => {
                        for mut event in watch_response.take_events() {
                            if let Some(kvs) = event.take_kvs() {
                                let k = String::from(&kvs.key_str()[root.clone().len()..]); // remove root
                                let v = kvs.value_str().to_string();
                                let event = match event.event_type() {
                                    EventType::Put => Event {
                                        event_type: DEventType::Put,
                                        key: k,
                                        value: v,
                                    },
                                    EventType::Delete => Event {
                                        event_type: DEventType::Delete,
                                        key: k,
                                        value: v,
                                    },
                                };

                                info!("sent: event={:?}", &event);
                                match sender.send(event) {
                                    Ok(_) => (),
                                    Err(e) => {
                                        error!("failed to send message: error={:?}", e);
                                    }
                                };
                            }
                        }
                    }
                    Err(e) => {
                        error!("failed to get watch response: error={:?}", e);
                    }
                };
            }

            watching.store(false, Ordering::Relaxed);
            info!("stop watch thread");
        });

        Ok(())
    }

    async fn unwatch(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        if !self.watching.load(Ordering::Relaxed) {
            let msg = "watch thread is not running";
            warn!("{}", msg);
            return Err(Box::new(IOError::new(ErrorKind::Other, msg)));
        }

        match self.client.shutdown().await {
            Ok(_) => Ok(()),
            Err(err) => {
                let msg = format!("failed to shutdown etcd client: error={:?}", err);
                error!("{}", msg);
                Err(Box::new(err))
            }
        }
    }
}
