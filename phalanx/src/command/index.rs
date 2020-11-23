use std::convert::Infallible;
use std::net::ToSocketAddrs;
use std::thread::sleep;
use std::time::Duration;

use clap::ArgMatches;
use hyper::service::{make_service_fn, service_fn};
use hyper::Server as HyperServer;
use log::*;
use tokio::signal;
use tonic::transport::Server as TonicServer;
use tonic::Request;

use phalanx_common::error::{Error, ErrorKind};
use phalanx_common::log::set_logger;
use phalanx_common::path::expand_tilde;
use phalanx_discovery::discovery::etcd::{
    Etcd as EtcdDiscovery, EtcdConfig, TYPE as ETCD_DISCOVERY_TYPE,
};
use phalanx_discovery::discovery::nop::{Nop as NopDiscovery, TYPE as NOP_DISCOVERY_TYPE};
use phalanx_discovery::discovery::DiscoveryContainer;
use phalanx_index::index::config::IndexConfig;
use phalanx_index::index::watcher::Watcher;
use phalanx_index::server::grpc::IndexService;
use phalanx_index::server::http::handle;
use phalanx_proto::phalanx::index_service_client::IndexServiceClient;
use phalanx_proto::phalanx::index_service_server::IndexServiceServer;
use phalanx_proto::phalanx::{NodeDetails, Role, State, UnwatchReq, WatchReq};
use phalanx_storage::storage::minio::{
    Minio as MinioStorage, MinioConfig, TYPE as MINIO_STORAGE_TYPE,
};
use phalanx_storage::storage::nop::{Nop as NopStorage, TYPE as NOP_STORAGE_TYPE};
use phalanx_storage::storage::StorageContainer;

pub async fn run_index(matches: &ArgMatches<'_>) -> Result<(), Error> {
    set_logger();

    let index_directory = match matches.value_of("index_directory") {
        Some(index_directory) => match expand_tilde(index_directory) {
            Some(index_directory) => index_directory,
            None => {
                return Err(Error::from(ErrorKind::Cli(format!(
                    "expand tilde error: {}",
                    index_directory
                ))));
            }
        },
        None => {
            return Err(Error::from(ErrorKind::Cli(
                "missing --index-directory".to_string(),
            )))
        }
    };
    let index_directory = match index_directory.to_str() {
        Some(index_directory) => index_directory,
        None => {
            return Err(Error::from(ErrorKind::Cli(format!(
                "failed to convert str: {:?}",
                index_directory
            ))));
        }
    };

    let schema_file = match matches.value_of("schema_file") {
        Some(schema_file) => match expand_tilde(schema_file) {
            Some(schema_file) => schema_file,
            None => {
                return Err(Error::from(ErrorKind::Cli(format!(
                    "expand tilde error: {}",
                    schema_file
                ))));
            }
        },
        None => {
            return Err(Error::from(ErrorKind::Cli(
                "missing --schema-file".to_string(),
            )))
        }
    };
    let schema_file = match schema_file.to_str() {
        Some(schema_file) => schema_file,
        None => {
            return Err(Error::from(ErrorKind::Cli(format!(
                "failed to convert str: {:?}",
                schema_file
            ))));
        }
    };

    let unique_key_field = match matches.value_of("unique_id_field") {
        Some(unique_key_field) => unique_key_field,
        None => {
            return Err(Error::from(ErrorKind::Cli(
                "missing --unique-id-field".to_string(),
            )))
        }
    };

    let tokenizer_file = match matches.value_of("tokenizer_file") {
        Some(tokenizer_file) => match expand_tilde(tokenizer_file) {
            Some(tokenizer_file) => tokenizer_file,
            None => {
                return Err(Error::from(ErrorKind::Cli(format!(
                    "expand tilde error: {}",
                    tokenizer_file
                ))));
            }
        },
        None => {
            return Err(Error::from(ErrorKind::Cli(
                "missing --tokenizer-file".to_string(),
            )))
        }
    };
    let tokenizer_file = match tokenizer_file.to_str() {
        Some(tokenizer_file) => tokenizer_file,
        None => {
            return Err(Error::from(ErrorKind::Cli(format!(
                "failed to convert str: {:?}",
                tokenizer_file
            ))));
        }
    };

    let indexer_threads = match matches.value_of("indexer_threads") {
        Some(indexer_threads) => match indexer_threads.parse::<usize>() {
            Ok(indexer_threads) => indexer_threads,
            Err(e) => {
                return Err(Error::from(ErrorKind::Cli(format!(
                    "failed to parse indexer threads {}: {}",
                    indexer_threads,
                    e.to_string()
                ))));
            }
        },
        None => {
            return Err(Error::from(ErrorKind::Cli(
                "missing --indexer-threads".to_string(),
            )))
        }
    };

    let indexer_memory_size = match matches.value_of("indexer_memory_size") {
        Some(indexer_memory_size) => match indexer_memory_size.parse::<usize>() {
            Ok(indexer_memory_size) => indexer_memory_size,
            Err(e) => {
                return Err(Error::from(ErrorKind::Cli(format!(
                    "failed to parse indexer memory size {}: {}",
                    indexer_memory_size,
                    e.to_string()
                ))));
            }
        },
        None => {
            return Err(Error::from(ErrorKind::Cli(
                "missing --indexer-memory-size".to_string(),
            )));
        }
    };

    // create index
    let index_config = IndexConfig {
        index_dir: String::from(index_directory),
        schema_file: String::from(schema_file),
        unique_key_field: String::from(unique_key_field),
        tokenizer_file: String::from(tokenizer_file),
        indexer_threads,
        indexer_memory_size,
        ..Default::default()
    };

    let mut discovery_container = match matches.value_of("discovery_type") {
        Some(discovery_type) => {
            let discovery_root = match matches.value_of("discovery_root") {
                Some(discovery_root) => discovery_root,
                None => {
                    return Err(Error::from(ErrorKind::Cli(
                        "missing --discovery-root".to_string(),
                    )))
                }
            };

            match discovery_type {
                ETCD_DISCOVERY_TYPE => {
                    let etcd_endpoints: Vec<String> = match matches.values_of("etcd_endpoints") {
                        Some(etcd_endpoints) => {
                            etcd_endpoints.map(|addr| addr.to_string()).collect()
                        }
                        None => {
                            return Err(Error::from(ErrorKind::Cli(
                                "missing --etcd-endpoints".to_string(),
                            )))
                        }
                    };

                    let config = EtcdConfig {
                        endpoints: etcd_endpoints,
                        root: Some(discovery_root.to_string()),
                        auth: None,
                        tls_ca_path: None,
                        tls_cert_path: None,
                        tls_key_path: None,
                    };
                    DiscoveryContainer {
                        discovery: Box::new(EtcdDiscovery::new(config)),
                    }
                }
                NOP_DISCOVERY_TYPE => DiscoveryContainer {
                    discovery: Box::new(NopDiscovery::new()),
                },
                _ => {
                    return Err(Error::from(ErrorKind::Cli(format!(
                        "unsupported discovery type: {}",
                        discovery_type
                    ))));
                }
            }
        }
        None => DiscoveryContainer {
            discovery: Box::new(NopDiscovery::new()),
        },
    };

    let storage_container = match matches.value_of("storage_type") {
        Some(storage_type) => {
            let storage_bucket = match matches.value_of("storage_bucket") {
                Some(storage_bucket) => storage_bucket,
                None => {
                    return Err(Error::from(ErrorKind::Cli(
                        "missing --storage-bucket".to_string(),
                    )))
                }
            };
            match storage_type {
                MINIO_STORAGE_TYPE => {
                    let minio_access_key = match matches.value_of("minio_access_key") {
                        Some(minio_access_key) => minio_access_key,
                        None => {
                            return Err(Error::from(ErrorKind::Cli(
                                "missing --minio-access-key".to_string(),
                            )))
                        }
                    };
                    let minio_secret_key = match matches.value_of("minio_secret_key") {
                        Some(minio_secret_key) => minio_secret_key,
                        None => {
                            return Err(Error::from(ErrorKind::Cli(
                                "missing --minio-secret-key".to_string(),
                            )))
                        }
                    };
                    let minio_endpoint = match matches.value_of("minio_endpoint") {
                        Some(minio_endpoint) => minio_endpoint,
                        None => {
                            return Err(Error::from(ErrorKind::Cli(
                                "missing --minio-endpoint".to_string(),
                            )))
                        }
                    };

                    let config = MinioConfig {
                        access_key: minio_access_key.to_string(),
                        secret_key: minio_secret_key.to_string(),
                        endpoint: minio_endpoint.to_string(),
                        bucket: storage_bucket.to_string(),
                    };

                    StorageContainer {
                        storage: Box::new(MinioStorage::new(config)),
                    }
                }
                NOP_STORAGE_TYPE => StorageContainer {
                    storage: Box::new(NopStorage::new()),
                },
                _ => {
                    return Err(Error::from(ErrorKind::Cli(format!(
                        "unsupported storage type: {}",
                        storage_type
                    ))));
                }
            }
        }
        None => StorageContainer {
            storage: Box::new(NopStorage::new()),
        },
    };

    let address = match matches.value_of("address") {
        Some(host) => host,
        None => return Err(Error::from(ErrorKind::Cli("missing --address".to_string()))),
    };

    let grpc_port = match matches.value_of("grpc_port") {
        Some(grpc_port) => match grpc_port.parse::<u16>() {
            Ok(grpc_port) => grpc_port,
            Err(e) => {
                return Err(Error::from(ErrorKind::Cli(format!(
                    "failed to parse gRPC port {}: {}",
                    grpc_port,
                    e.to_string()
                ))));
            }
        },
        None => {
            return Err(Error::from(ErrorKind::Cli(
                "missing --grpc-port".to_string(),
            )))
        }
    };

    let http_port = match matches.value_of("http_port") {
        Some(http_port) => match http_port.parse::<u16>() {
            Ok(http_port) => http_port,
            Err(e) => {
                return Err(Error::from(ErrorKind::Cli(format!(
                    "failed to parse HTTP port {}: {}",
                    http_port,
                    e.to_string()
                ))));
            }
        },
        None => {
            return Err(Error::from(ErrorKind::Cli(
                "missing --http-port".to_string(),
            )))
        }
    };

    let index_name = match matches.value_of("index_name") {
        Some(index_name) => index_name,
        None => {
            return Err(Error::from(ErrorKind::Cli(
                "missing --index-name".to_string(),
            )))
        }
    };

    let shard_name = match matches.value_of("shard_name") {
        Some(shard_name) => shard_name,
        None => {
            return Err(Error::from(ErrorKind::Cli(
                "missing --shard-name".to_string(),
            )))
        }
    };

    let node_name = match matches.value_of("node_name") {
        Some(node_name) => {
            if !node_name.starts_with("_") {
                node_name
            } else {
                return Err(Error::from(ErrorKind::Cli(
                    "node name starts with '_' is not allowed".to_string(),
                )));
            }
        }
        None => {
            return Err(Error::from(ErrorKind::Cli(
                "missing --node-name".to_string(),
            )))
        }
    };

    // resolve gRPC address
    let grpc_address = format!("{}:{}", address, grpc_port);
    let grpc_address = match grpc_address.to_socket_addrs() {
        Ok(mut grpc_sock_addrs) => match grpc_sock_addrs.next() {
            Some(grpc_sock_addr) => grpc_sock_addr,
            None => {
                return Err(Error::from(ErrorKind::Cli(format!(
                    "failed to resolve address {:?}",
                    grpc_sock_addrs,
                ))));
            }
        },
        Err(e) => {
            return Err(Error::from(ErrorKind::Cli(format!(
                "failed to convert socket address {}: {}",
                grpc_address,
                e.to_string()
            ))));
        }
    };

    // resolve HTTP address
    let http_address = format!("{}:{}", address, http_port);
    let http_address = match http_address.to_socket_addrs() {
        Ok(mut http_sock_addrs) => match http_sock_addrs.next() {
            Some(http_sock_addr) => http_sock_addr,
            None => {
                return Err(Error::from(ErrorKind::Cli(format!(
                    "failed to resolve address {:?}",
                    http_sock_addrs,
                ))));
            }
        },
        Err(e) => {
            return Err(Error::from(ErrorKind::Cli(format!(
                "failed to convert socket address {}: {}",
                http_address,
                e.to_string()
            ))));
        }
    };

    // register this node
    let key = format!("/{}/{}/{}.json", index_name, shard_name, node_name);
    let node_details = NodeDetails {
        address: grpc_address.to_string(),
        state: State::Disconnected as i32,
        role: Role::Candidate as i32,
    };
    let node_details_json = match serde_json::to_string(&node_details) {
        Ok(node_details_json) => node_details_json,
        Err(e) => {
            return Err(Error::from(ErrorKind::Cli(format!(
                "failed to serialize node details {:?}: {}",
                &node_details,
                e.to_string()
            ))));
        }
    };
    match discovery_container
        .discovery
        .put(key.as_str(), node_details_json.as_str())
        .await
    {
        Ok(_) => (),
        Err(e) => {
            return Err(Error::from(ErrorKind::Cli(format!(
                "failed to register node {}: {}",
                &key,
                e.to_string()
            ))));
        }
    };

    // node watcher
    let watcher = Watcher::new(
        index_name,
        shard_name,
        node_name,
        discovery_container,
        index_directory,
        storage_container.clone(),
    );

    // start index service using gRPC
    let index_service = IndexService::new(index_config, watcher);
    tokio::spawn(
        TonicServer::builder()
            .add_service(IndexServiceServer::new(index_service))
            .serve(grpc_address),
    );
    info!("start gRPC server on {}", grpc_address.to_string());

    // create gRPC client
    let mut grpc_client =
        match IndexServiceClient::connect(format!("http://{}", grpc_address.to_string())).await {
            Ok(grpc_client) => grpc_client,
            Err(e) => {
                return Err(Error::from(ErrorKind::Cli(format!(
                    "failed to connect gRPC server {}: {}",
                    grpc_address.to_string(),
                    e.to_string()
                ))));
            }
        };

    // start http service
    let grpc_client2 = grpc_client.clone();
    let http_service = make_service_fn(move |_| {
        let grpc_client = grpc_client2.clone();
        async move { Ok::<_, Infallible>(service_fn(move |req| handle(grpc_client.clone(), req))) }
    });
    tokio::spawn(HyperServer::bind(&http_address).serve(http_service));
    info!("start HTTP server on {}", http_address.to_string());

    // watch
    let watch_req = Request::new(WatchReq { interval: 0 });
    match grpc_client.watch(watch_req).await {
        Ok(_) => (),
        Err(e) => {
            return Err(Error::from(ErrorKind::Cli(format!(
                "failed to watch index node {}: {}",
                grpc_address.to_string(),
                e.to_string()
            ))));
        }
    };

    signal::ctrl_c().await.unwrap();
    info!("ctrl-c received");

    // unwatch
    let unwatch_req = Request::new(UnwatchReq {});
    match grpc_client.unwatch(unwatch_req).await {
        Ok(_) => (),
        Err(e) => {
            return Err(Error::from(ErrorKind::Cli(format!(
                "failed to unwatch index node {}: {}",
                grpc_address.to_string(),
                e.to_string()
            ))));
        }
    };

    sleep(Duration::from_secs(1));

    Ok(())
}
