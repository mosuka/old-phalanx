use std::error::Error;
use std::fs;
use std::io::{Error as IOError, ErrorKind};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use crossbeam::channel::{unbounded, TryRecvError};
use lazy_static::lazy_static;
use log::*;
use notify::event::{ModifyKind, RenameMode};
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher as NWatcher};
use phalanx_discovery::discovery::{DiscoveryContainer, EventType};
use phalanx_storage::storage::StorageContainer;
use regex::Regex;
use serde_json::Value;
use tokio::fs::File;
use tokio::io::{copy, AsyncReadExt};
use tokio::sync::RwLock;
use tonic::codegen::Arc;
use walkdir::WalkDir;

use phalanx_discovery::discovery::etcd::{Etcd, EtcdConfig, TYPE as ETCD_TYPE};
use phalanx_discovery::discovery::nop::Nop;
use phalanx_proto::phalanx::{NodeDetails, Role, State};

lazy_static! {
    static ref KEY_REGEX: Regex = Regex::new(r"^/([^/]+)/([^/]+)/([^/]+)\.json").unwrap();
}

fn parse_key(key: &str) -> Result<(String, String, String), IOError> {
    match KEY_REGEX.captures(key) {
        Some(cap) => {
            let index_name = match cap.get(1) {
                Some(m) => m.as_str(),
                None => {
                    return Err(IOError::new(
                        ErrorKind::Other,
                        format!("index name doesn't match: key={}", key),
                    ))
                }
            };

            let shard_name = match cap.get(2) {
                Some(m) => m.as_str(),
                None => {
                    return Err(IOError::new(
                        ErrorKind::Other,
                        format!("shard name doesn't match: key={}", key),
                    ))
                }
            };

            let node_name = match cap.get(3) {
                Some(m) => m.as_str(),
                None => {
                    return Err(IOError::new(
                        ErrorKind::Other,
                        format!("node name doesn't match: key={}", key),
                    ))
                }
            };

            Ok((
                index_name.to_string(),
                shard_name.to_string(),
                node_name.to_string(),
            ))
        }
        None => Err(IOError::new(
            ErrorKind::Other,
            "no match for the node meta key",
        )),
    }
}

pub struct Watcher {
    index_name: String,
    shard_name: String,
    node_name: String,
    discovery_container: DiscoveryContainer,
    receiving: Arc<AtomicBool>,
    unreceive: Arc<AtomicBool>,
    node_details: Arc<RwLock<NodeDetails>>,
    index_dir: String,
    storage_container: StorageContainer,
    unwatch_local: Arc<AtomicBool>,
}

impl Watcher {
    pub fn new(
        index_name: &str,
        shard_name: &str,
        node_name: &str,
        discovery_container: DiscoveryContainer,
        index_dir: &str,
        storage_container: StorageContainer,
    ) -> Watcher {
        let node_details = NodeDetails {
            address: String::from(""),
            state: State::Disconnected as i32,
            role: Role::Candidate as i32,
        };

        Watcher {
            index_name: String::from(index_name),
            shard_name: String::from(shard_name),
            node_name: String::from(node_name),
            discovery_container,
            receiving: Arc::new(AtomicBool::new(false)),
            unreceive: Arc::new(AtomicBool::new(false)),
            node_details: Arc::new(RwLock::new(node_details)),
            index_dir: String::from(index_dir),
            storage_container,
            unwatch_local: Arc::new(AtomicBool::new(false)),
        }
    }

    pub async fn watch(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        if self.receiving.load(Ordering::Relaxed) {
            let msg = "receiver is already running";
            warn!("{}", msg);
            return Err(Box::new(IOError::new(ErrorKind::Other, msg)));
        }

        // initialize sender and receiver
        let (sender, receiver) = unbounded();

        // specifies the key of the shard to which it joined
        let key = format!("/{}/{}/", self.index_name, self.shard_name);
        match self
            .discovery_container
            .discovery
            .watch(sender, key.as_str())
            .await
        {
            Ok(_) => (),
            Err(err) => {
                let msg = format!("failed to watch: key={}, error={:?}", key, err);
                error!("{}", msg);
                return Err(err);
            }
        }

        let my_index_name = self.index_name.clone();
        let my_shard_name = self.shard_name.clone();
        let my_node_name = self.node_name.clone();
        let node_details = Arc::clone(&self.node_details);

        // prepare receiver
        let receiving = Arc::clone(&self.receiving);
        let unreceive = Arc::clone(&self.unreceive);

        let index_dir = self.index_dir.clone();
        let storage_container = self.storage_container.clone();

        tokio::spawn(async move {
            info!("start receive thread");
            receiving.store(true, Ordering::Relaxed);

            let storage_container = storage_container.clone();

            loop {
                match receiver.try_recv() {
                    Ok(event) => {
                        let (index_name, shard_name, node_name) = match parse_key(&event.key) {
                            Ok(names) => names,
                            Err(e) => {
                                // ignore keys that do not match the pattern
                                debug!("parse error: error={:?}", e);
                                continue;
                            }
                        };

                        if index_name == my_index_name
                            && shard_name == my_shard_name
                            && node_name == my_node_name
                        {
                            info!("node mata has been changed: event={:?}", event.clone());
                            match event.event_type {
                                EventType::Put => {
                                    match serde_json::from_str::<NodeDetails>(event.value.as_str())
                                    {
                                        Ok(nd) => {
                                            info!(
                                                "set local node cash: node_details={:?}",
                                                &node_details
                                            );
                                            let mut m = node_details.write().await;
                                            m.address = nd.address;
                                            m.state = nd.state;
                                            m.role = nd.role;
                                        }
                                        Err(err) => {
                                            error!("failed to parse JSON: error={:?}", err);
                                        }
                                    };
                                    info!("{:?}", event);
                                }
                                EventType::Delete => {
                                    info!("{:?}", event);
                                }
                            }
                        } else if index_name == my_index_name
                            && shard_name == my_shard_name
                            && node_name == "_index_meta"
                        {
                            info!("index mata has been changed: key={:?}", &event.key);

                            // replica node only
                            let m = node_details.read().await;
                            if m.role != Role::Replica as i32 {
                                continue;
                            }

                            // list segment ids
                            let mut segment_ids = Vec::new();
                            let meta_json_key =
                                format!("{}/{}/meta.json", &my_index_name, &my_shard_name);
                            let content =
                                match storage_container.storage.get(meta_json_key.as_str()).await {
                                    Ok(resp) => match resp {
                                        Some(content) => content,
                                        None => {
                                            error!("content is None");
                                            continue;
                                        }
                                    },
                                    Err(err) => {
                                        error!("failed to get .managed.json : error={:?}", err);
                                        continue;
                                    }
                                };
                            let value: Value =
                                match serde_json::from_slice::<Value>(&content.as_slice()) {
                                    Ok(value) => value,
                                    Err(err) => {
                                        error!("failed to parse meta.json: error={:?}", err);
                                        continue;
                                    }
                                };
                            let segments = match value.as_object().unwrap()["segments"].as_array() {
                                Some(segments) => segments,
                                None => {
                                    error!("segments element does not exist");
                                    continue;
                                }
                            };
                            for segment in segments {
                                let mut segment_id =
                                    match segment.as_object().unwrap()["segment_id"].as_str() {
                                        Some(segment_id) => segment_id.to_string(),
                                        None => {
                                            error!("segment_id element does not exist");
                                            continue;
                                        }
                                    };
                                // retain chars except '-'
                                // ex) "dba8a6c1-bee0-463b-9748-51e3664f856f" -> "dba8a6c1bee0463b974851e3664f856f"
                                segment_id.retain(|c| c != '-');
                                segment_ids.push(segment_id);
                            }

                            // list object names
                            let mut object_names = Vec::new();
                            let prefix = format!("{}/{}/", &my_index_name, &my_shard_name);
                            match storage_container.storage.list(&prefix).await {
                                Ok(full_object_names) => {
                                    for full_object_name in full_object_names {
                                        let object_name =
                                            &full_object_name.as_str()[prefix.len()..];
                                        match Path::new(object_name).file_stem() {
                                            Some(file_stem) => match file_stem.to_str() {
                                                Some(file_stem) => {
                                                    if segment_ids.contains(&file_stem.to_string())
                                                    {
                                                        object_names
                                                            .push(String::from(object_name));
                                                    }
                                                }
                                                None => {
                                                    continue;
                                                }
                                            },
                                            None => {
                                                continue;
                                            }
                                        };
                                    }
                                }
                                Err(e) => {
                                    error!(
                                        "failed to list object names under {} : error={:?}",
                                        &prefix, e
                                    );
                                    continue;
                                }
                            };
                            object_names.push(".managed.json".to_string());
                            object_names.push("meta.json".to_string());

                            // pull objects
                            for object_name in &object_names {
                                // get object
                                let object_key = format!(
                                    "{}/{}/{}",
                                    &my_index_name, &my_shard_name, object_name
                                );
                                match storage_container.storage.get(object_key.as_str()).await {
                                    Ok(resp) => {
                                        match resp {
                                            Some(content) => {
                                                let file_path =
                                                    Path::new(&index_dir).join(object_name);
                                                info!(
                                                    "pull {} to {}",
                                                    &object_key,
                                                    &file_path.to_str().unwrap()
                                                );
                                                let mut file =
                                                    match File::create(&file_path).await {
                                                        Ok(file) => file,
                                                        Err(err) => {
                                                            error!(
                                                            "failed to create file {}: error={:?}",
                                                            &file_path.to_str().unwrap(), err
                                                        );
                                                            continue;
                                                        }
                                                    };
                                                copy(&mut content.as_slice(), &mut file)
                                                    .await
                                                    .unwrap();
                                            }
                                            None => {
                                                error!("content is None");
                                                continue;
                                            }
                                        };
                                    }
                                    Err(err) => {
                                        error!(
                                            "failed to pull object {}: error={:?}",
                                            &object_key, err
                                        );
                                        continue;
                                    }
                                };
                            }

                            // list file names
                            let mut file_names = Vec::new();
                            for entry in WalkDir::new(&index_dir)
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

                            // remove unnecessary files
                            for file_name in file_names {
                                if !object_names.contains(&file_name) {
                                    let file_path = String::from(
                                        Path::new(&index_dir).join(&file_name).to_str().unwrap(),
                                    );
                                    match fs::remove_file(&file_path) {
                                        Ok(()) => info!("delete: {}", &file_path),
                                        Err(err) => {
                                            error!("failed to delete file: error={:?}", err);
                                            continue;
                                        }
                                    };
                                }
                            }
                        }
                    }
                    Err(TryRecvError::Disconnected) => {
                        info!("channel disconnected");
                        break;
                    }
                    Err(TryRecvError::Empty) => {
                        if unreceive.load(Ordering::Relaxed) {
                            info!("receive a stop signal");
                            // restore unreceive to false
                            unreceive.store(false, Ordering::Relaxed);
                            break;
                        }
                    }
                }
            }

            receiving.store(false, Ordering::Relaxed);
            info!("stop receive thread");
        });

        let config_json = self
            .discovery_container
            .discovery
            .export_config_json()
            .unwrap_or("".to_string());
        let discovery_container = match self.discovery_container.discovery.get_type() {
            ETCD_TYPE => {
                let config = match serde_json::from_str::<EtcdConfig>(config_json.as_str()) {
                    Ok(config) => config,
                    Err(err) => {
                        let msg = format!("failed to  parse config JSON: error={:?}", err);
                        error!("{}", msg);
                        return Err(Box::new(IOError::new(ErrorKind::Other, msg)));
                    }
                };
                DiscoveryContainer {
                    discovery: Box::new(Etcd::new(config)),
                }
            }
            _ => DiscoveryContainer {
                discovery: Box::new(Nop::new()),
            },
        };
        // let discovery_container = self.discovery_container.clone();

        let storage_container2 = self.storage_container.clone();

        let index_dir2 = self.index_dir.clone();
        let my_index_name2 = self.index_name.clone();
        let my_shard_name2 = self.shard_name.clone();

        let node_details2 = Arc::clone(&self.node_details);

        let unwatch_local = Arc::clone(&self.unwatch_local);
        let watch_file = Path::new(self.index_dir.clone().as_str()).join("meta.json");
        let watch_dir = self.index_dir.clone();
        tokio::spawn(async move {
            info!("start local index watch thread");

            let (sender, receiver) = unbounded();

            let mut watcher: RecommendedWatcher =
                NWatcher::new_immediate(move |res| sender.send(res).unwrap()).unwrap();

            watcher.watch(watch_dir, RecursiveMode::Recursive).unwrap();

            loop {
                let mut discovery_container = discovery_container.clone();

                match receiver.try_recv() {
                    Ok(msg) => {
                        // primary node only
                        let m = node_details2.read().await;
                        if m.role != Role::Primary as i32 {
                            continue;
                        }

                        let event: Event = match msg {
                            Ok(event) => event,
                            Err(err) => {
                                error!("failed to receive event: err={:?}", err);
                                continue;
                            }
                        };

                        // catch a renaming event to meta.json
                        if event.kind == EventKind::Modify(ModifyKind::Name(RenameMode::Both))
                            && event.paths.last().unwrap() == &watch_file
                        {
                            info!("meta.json has been changed: {:?}", event);

                            // list segment ids
                            let mut segment_ids = Vec::new();
                            let path = Path::new(&index_dir2).join("meta.json");
                            let mut file = File::open(&path).await.unwrap();
                            let mut content = String::new();
                            file.read_to_string(&mut content).await.unwrap();
                            let value = serde_json::from_str::<Value>(content.as_str()).unwrap();
                            for segment in
                                value.as_object().unwrap()["segments"].as_array().unwrap()
                            {
                                let mut segment_id = segment.as_object().unwrap()["segment_id"]
                                    .as_str()
                                    .unwrap()
                                    .to_string();
                                // retain chars except '-'
                                // ex) "dba8a6c1-bee0-463b-9748-51e3664f856f" -> "dba8a6c1bee0463b974851e3664f856f"
                                segment_id.retain(|c| c != '-');
                                segment_ids.push(segment_id);
                            }

                            // list file names
                            let mut file_names = Vec::new();
                            for entry in WalkDir::new(&index_dir2)
                                .follow_links(true)
                                .into_iter()
                                .filter_map(|e| e.ok())
                                .filter(|e| e.file_type().is_file())
                            {
                                match entry.file_name().to_str() {
                                    Some(file_name) => {
                                        match Path::new(file_name).file_stem() {
                                            Some(file_stem) => match file_stem.to_str() {
                                                Some(file_stem) => {
                                                    if segment_ids.contains(&file_stem.to_string())
                                                    {
                                                        file_names.push(String::from(file_name));
                                                    }
                                                }
                                                None => {
                                                    continue;
                                                }
                                            },
                                            None => {
                                                continue;
                                            }
                                        };
                                    }
                                    None => {
                                        continue;
                                    }
                                };
                            }
                            file_names.push(String::from(".managed.json"));
                            file_names.push(String::from("meta.json"));

                            // push files to object storage
                            for file_name in &file_names {
                                // read file
                                let file_path = String::from(
                                    Path::new(&index_dir2).join(&file_name).to_str().unwrap(),
                                );

                                let mut file = match File::open(&file_path).await {
                                    Ok(file) => file,
                                    Err(e) => {
                                        error!("failed to open file: error={:?}", e);
                                        continue;
                                    }
                                };
                                let mut content: Vec<u8> = Vec::new();
                                match file.read_to_end(&mut content).await {
                                    Ok(_) => (),
                                    Err(e) => {
                                        error!("failed to read file: error={:?}", e);
                                        continue;
                                    }
                                };

                                // put object
                                let object_key = format!(
                                    "{}/{}/{}",
                                    &my_index_name2, &my_shard_name2, &file_name
                                );
                                info!("put {} to {}", &file_path, &object_key);
                                match storage_container2
                                    .storage
                                    .set(object_key.as_str(), content.as_slice())
                                    .await
                                {
                                    Ok(_) => (),
                                    Err(e) => {
                                        error!("failed to put object: error={:?}", e);
                                        continue;
                                    }
                                };
                            }

                            // list object names
                            let prefix = format!("{}/{}/", &my_index_name2, &my_shard_name2);
                            let object_names = match storage_container2.storage.list(&prefix).await
                            {
                                Ok(object_keys) => {
                                    let mut object_names = Vec::new();
                                    for object_key in object_keys {
                                        // cluster1/shard1/meta.json -> meta.json
                                        let object_name =
                                            Path::new(&object_key).strip_prefix(&prefix).unwrap();
                                        object_names
                                            .push(String::from(object_name.to_str().unwrap()));
                                    }
                                    object_names
                                }
                                Err(e) => {
                                    error!("failed to list object name: error={:?}", e);
                                    continue;
                                }
                            };

                            // remove unnecessary objects
                            for object_name in object_names {
                                if !file_names.contains(&object_name) {
                                    // e.g. meta.json -> cluster1/shard1/meta.json
                                    let object_key = format!(
                                        "{}/{}/{}",
                                        &my_index_name2, &my_shard_name2, &object_name
                                    );
                                    match storage_container2
                                        .storage
                                        .delete(object_key.as_str())
                                        .await
                                    {
                                        Ok(_output) => (),
                                        Err(e) => {
                                            error!("failed to delete object: error={:?}", e);
                                            continue;
                                        }
                                    };
                                }
                            }

                            // put index metadata
                            let key = format!(
                                "/{}/{}/_index_meta.json",
                                my_index_name2.as_str(),
                                my_shard_name2.as_str()
                            );

                            let mut file = match File::open(&watch_file).await {
                                Ok(file) => file,
                                Err(e) => {
                                    error!("failed to open file: error={:?}", e);
                                    continue;
                                }
                            };
                            let mut value = String::new();
                            match file.read_to_string(&mut value).await {
                                Ok(_) => (),
                                Err(e) => {
                                    error!("failed to read file: error={:?}", e);
                                    continue;
                                }
                            };

                            match discovery_container
                                .discovery
                                .put(key.as_str(), value.as_str())
                                .await
                            {
                                Ok(_) => {
                                    info!("put index metadata: key={}", &key);
                                }
                                Err(e) => {
                                    error!("failed to put _index_meta.json: error={:?}", e);
                                    continue;
                                }
                            };
                        }
                    }
                    Err(TryRecvError::Disconnected) => {
                        info!("watch local file channel disconnected");
                        break;
                    }
                    Err(TryRecvError::Empty) => {
                        if unwatch_local.load(Ordering::Relaxed) {
                            info!("receive a stop watch local file signal");
                            // restore unwatch_local to false
                            unwatch_local.store(false, Ordering::Relaxed);
                            break;
                        }
                    }
                }
            }

            info!("stop local index watch thread");
        });

        Ok(())
    }

    pub async fn unwatch(&mut self) -> Result<(), Box<dyn Error + Send + Sync>> {
        if !self.receiving.load(Ordering::Relaxed) {
            let msg = "receiver is not running";
            warn!("{}", msg);
            return Err(Box::new(IOError::new(ErrorKind::Other, msg)));
        }

        match self.discovery_container.discovery.unwatch().await {
            Ok(_) => (),
            Err(err) => {
                let msg = format!("failed to unwatch: error={:?}", err);
                error!("{}", msg);
                return Err(err);
            }
        }

        self.unreceive.store(true, Ordering::Relaxed);

        self.unwatch_local.store(true, Ordering::Relaxed);

        Ok(())
    }

    pub async fn get_node_meta(&self) -> NodeDetails {
        let node_details = Arc::clone(&self.node_details);
        let node_details = node_details.read().await;

        NodeDetails {
            address: node_details.address.clone(),
            state: node_details.state,
            role: node_details.role,
        }
    }
}