use std::convert::Infallible;
use std::error::Error;
use std::io::{Error as IOError, ErrorKind};
use std::net::ToSocketAddrs;
use std::thread::sleep;
use std::time::Duration;

use clap::{crate_authors, crate_name, crate_version, App, AppSettings, Arg};
use hyper::service::{make_service_fn, service_fn};
use hyper::Server as HyperServer;
use log::*;
use tokio::signal;
use tonic::transport::Server as TonicServer;
use tonic::Request;

use phalanx_common::log::set_logger;
use phalanx_common::path::expand_tilde;
use phalanx_discovery::discovery::etcd::{
    Etcd as EtcdDiscovery, EtcdConfig, DEFAULT_ENDPOINTS, TYPE as ETCD_DISCOVERY_TYPE,
};
use phalanx_discovery::discovery::nop::Nop as NopDiscovery;
use phalanx_discovery::discovery::{DiscoveryContainer, DEFAULT_ROOT};
use phalanx_index::index::config::{
    IndexConfig, DEFAULT_INDEXER_MEMORY_SIZE, DEFAULT_INDEX_DIRECTORY, DEFAULT_SCHEMA_FILE,
    DEFAULT_TOKENIZER_FILE, DEFAULT_UNIQUE_KEY_FIELD,
};
use phalanx_index::index::watcher::Watcher;
use phalanx_index::server::grpc::IndexService;
use phalanx_index::server::http::handle;
use phalanx_proto::phalanx::index_service_client::IndexServiceClient;
use phalanx_proto::phalanx::index_service_server::IndexServiceServer;
use phalanx_proto::phalanx::{NodeDetails, Role, State, UnwatchReq, WatchReq};
use phalanx_storage::storage::minio::{Minio, MinioConfig};
use phalanx_storage::storage::minio::{
    DEFAULT_ACCESS_KEY, DEFAULT_ENDPOINT, DEFAULT_SECRET_KEY, TYPE as MINIO_STORAGE_TYPE,
};
use phalanx_storage::storage::nop::Nop as NopStorage;
use phalanx_storage::storage::{StorageContainer, DEFAULT_BUCKET};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    set_logger();

    let default_indexer_threads = format!("{}", num_cpus::get().to_owned());
    let default_indexer_memory_size = format!("{}", DEFAULT_INDEXER_MEMORY_SIZE);

    let app = App::new(crate_name!())
        .setting(AppSettings::DeriveDisplayOrder)
        .version(crate_version!())
        .author(crate_authors!())
        .about("Phalanx index")
        .help_message("Prints help information.")
        .version_message("Prints version information.")
        .version_short("v")
        .arg(
            Arg::with_name("HOST")
                .help("Node address.")
                .long("host")
                .value_name("HOST")
                .default_value("0.0.0.0")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("GRPC_PORT")
                .help("gRPC port number")
                .long("grpc-port")
                .value_name("GRPC_PORT")
                .default_value("5000")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("HTTP_PORT")
                .help("HTTP port number")
                .long("http-port")
                .value_name("HTTP_PORT")
                .default_value("8000")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("INDEX_DIRECTORY")
                .help("Index directory. Stores index, snapshots, and raft logs.")
                .long("index-directory")
                .value_name("INDEX_DIRECTORY")
                .default_value(DEFAULT_INDEX_DIRECTORY)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("SCHEMA_FILE")
                .help("Schema file. Must specify An existing file name.")
                .long("schema-file")
                .value_name("SCHEMA_FILE")
                .default_value(DEFAULT_SCHEMA_FILE)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("TOKENIZER_FILE")
                .help("Tokenizer file. Must specify An existing file name.")
                .long("tokenizer-file")
                .value_name("TOKENIZER_FILE")
                .default_value(DEFAULT_TOKENIZER_FILE)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("INDEXER_THREADS")
                .help("Number of indexer threads. By default indexer uses number of available logical cpu as threads count.")
                .long("indexer-threads")
                .value_name("INDEXER_THREADS")
                .default_value(&default_indexer_threads)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("INDEXER_MEMORY_SIZE")
                .help("Total memory size (in bytes) used by the indexer.")
                .long("indexer-memory-size")
                .value_name("INDEXER_MEMORY_SIZE")
                .default_value(&default_indexer_memory_size)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("UNIQUE_ID_FIELD")
                .help("Unique id field name.")
                .long("unique-id-field")
                .value_name("UNIQUE_ID_FIELD")
                .default_value(DEFAULT_UNIQUE_KEY_FIELD)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("INDEX_NAME")
                .help("Index name.")
                .long("index-name")
                .value_name("INDEX_NAME")
                .default_value("index0")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("SHARD_NAME")
                .help("Shard name.")
                .long("shard-name")
                .value_name("SHARD_NAME")
                .default_value("shard0")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("NODE_NAME")
                .help("Node name.")
                .long("node-name")
                .value_name("NODE_NAME")
                .default_value("node0")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("DISCOVERY_TYPE")
                .help("Discovery type. Supported: etcd")
                .long("discovery-type")
                .value_name("DISCOVERY_TYPE")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("ETCD_ENDPOINTS")
                .help("The etcd endpoints. Use `,` to separate address. Example: `http://192.168.1.10:2379,http://192.168.1.11:2379,http://192.168.1.12:2379`")
                .long("etcd-endpoints")
                .value_name("ETCD_ENDPOINTS")
                .default_value(DEFAULT_ENDPOINTS)
                .multiple(true)
                .use_delimiter(true)
                .require_delimiter(true)
                .value_delimiter(",")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("ETCD_ROOT")
                .help("etcd root")
                .long("etcd-root")
                .value_name("ETCD_ROOT")
                .default_value(DEFAULT_ROOT)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("STORAGE_TYPE")
                .help("Object storage type. Supported object storage: minio")
                .long("storage-type")
                .value_name("STORAGE_TYPE")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("MINIO_ACCESS_KEY")
                .help("Access key for MinIO.")
                .long("minio-access-key")
                .value_name("MINIO_ACCESS_KEY")
                .default_value(DEFAULT_ACCESS_KEY)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("MINIO_SECRET_KEY")
                .help("Secret key for MinIO.")
                .long("minio-secret-key")
                .value_name("MINIO_SECRET_KEY")
                .default_value(DEFAULT_SECRET_KEY)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("MINIO_ENDPOINT")
                .help("MinIO endpoint.")
                .long("minio-endpoint")
                .value_name("MINIO_ENDPOINT")
                .default_value(DEFAULT_ENDPOINT)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("MINIO_BUCKET")
                .help("MinIO bucket.")
                .long("minio-bucket")
                .value_name("MINIO_BUCKET")
                .default_value(DEFAULT_BUCKET)
                .takes_value(true),
        );

    let matches = app.get_matches();

    let host = matches.value_of("HOST").unwrap();
    let grpc_port = matches
        .value_of("GRPC_PORT")
        .unwrap()
        .parse::<u16>()
        .unwrap();
    let http_port = matches
        .value_of("HTTP_PORT")
        .unwrap()
        .parse::<u16>()
        .unwrap();

    let index_directory_path = expand_tilde(matches.value_of("INDEX_DIRECTORY").unwrap()).unwrap();
    let index_directory = index_directory_path.to_str().unwrap();
    let schema_file_path = expand_tilde(matches.value_of("SCHEMA_FILE").unwrap()).unwrap();
    let schema_file = schema_file_path.to_str().unwrap();
    let unique_key_field = matches.value_of("UNIQUE_ID_FIELD").unwrap();
    let tokenizer_file_path = expand_tilde(matches.value_of("TOKENIZER_FILE").unwrap()).unwrap();
    let tokenizer_file = tokenizer_file_path.to_str().unwrap();
    let indexer_threads = matches
        .value_of("INDEXER_THREADS")
        .unwrap()
        .parse::<usize>()
        .unwrap();
    let indexer_memory_size = matches
        .value_of("INDEXER_MEMORY_SIZE")
        .unwrap()
        .parse::<usize>()
        .unwrap();

    let index_name = matches.value_of("INDEX_NAME").unwrap();
    let shard_name = matches.value_of("SHARD_NAME").unwrap();
    let node_name = matches.value_of("NODE_NAME").unwrap();

    let discovery_type = match matches.value_of("DISCOVERY_TYPE") {
        Some(discovery_type) => discovery_type,
        _ => "",
    };

    let etcd_endpoints: Vec<&str> = matches
        .values_of("ETCD_ENDPOINTS")
        .unwrap()
        .map(|addr| addr)
        .collect();
    let etcd_root = matches.value_of("ETCD_ROOT").unwrap();

    let storage_type = match matches.value_of("STORAGE_TYPE") {
        Some(storage_type) => storage_type,
        _ => "",
    };

    let minio_access_key = matches.value_of("MINIO_ACCESS_KEY").unwrap();
    let minio_secret_key = matches.value_of("MINIO_SECRET_KEY").unwrap();
    let minio_endpoint = matches.value_of("MINIO_ENDPOINT").unwrap();
    let minio_bucket = matches.value_of("MINIO_BUCKET").unwrap();

    // create discovery
    let mut discovery_container: DiscoveryContainer = match discovery_type {
        ETCD_DISCOVERY_TYPE => {
            let config = EtcdConfig {
                endpoints: etcd_endpoints.iter().map(|s| s.to_string()).collect(),
                root: Some(etcd_root.to_string()),
                auth: None,
                tls_ca_path: None,
                tls_cert_path: None,
                tls_key_path: None,
            };
            DiscoveryContainer {
                discovery: Box::new(EtcdDiscovery::new(config)),
            }
        }
        _ => DiscoveryContainer {
            discovery: Box::new(NopDiscovery::new()),
        },
    };

    // create storage
    let storage_container: StorageContainer = match storage_type {
        MINIO_STORAGE_TYPE => {
            let config = MinioConfig {
                access_key: minio_access_key.to_string(),
                secret_key: minio_secret_key.to_string(),
                endpoint: minio_endpoint.to_string(),
                bucket: minio_bucket.to_string(),
            };
            StorageContainer {
                storage: Box::new(Minio::new(config)),
            }
        }
        _ => StorageContainer {
            storage: Box::new(NopStorage::new()),
        },
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

    // register a node
    let key = format!("/{}/{}/{}.json", index_name, shard_name, node_name);
    let node_details = NodeDetails {
        address: format!("{}:{}", host, grpc_port)
            .to_socket_addrs()
            .unwrap()
            .next()
            .unwrap()
            .to_string(),
        state: State::Disconnected as i32,
        role: Role::Candidate as i32,
    };
    let json = serde_json::to_string(&node_details).unwrap();
    match discovery_container
        .discovery
        .put(key.as_str(), json.as_str())
        .await
    {
        Ok(_) => (),
        Err(e) => {
            return Err(e);
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

    // index service
    let index_service = IndexService::new(index_config, watcher);

    // start gRPC server
    let grpc_addr = format!("{}:{}", host, grpc_port)
        .to_socket_addrs()
        .unwrap()
        .next()
        .unwrap();
    tokio::spawn(
        TonicServer::builder()
            .add_service(IndexServiceServer::new(index_service))
            .serve(grpc_addr),
    );
    info!("start gRPC server on {}", grpc_addr.to_string());

    // create gRPC client
    let grpc_server_url = format!("http://{}", grpc_addr.to_string());
    let mut grpc_client = IndexServiceClient::connect(grpc_server_url.clone())
        .await
        .unwrap();

    // http service
    let http_addr = format!("{}:{}", host, http_port)
        .to_socket_addrs()
        .unwrap()
        .next()
        .unwrap();
    let grpc_client2 = grpc_client.clone();
    let http_service = make_service_fn(move |_| {
        let grpc_client = grpc_client2.clone();
        async move { Ok::<_, Infallible>(service_fn(move |req| handle(grpc_client.clone(), req))) }
    });

    // start HTTP server
    tokio::spawn(HyperServer::bind(&http_addr).serve(http_service));
    info!("start HTTP server on {}", http_addr.to_string());

    // watch
    let watch_req = Request::new(WatchReq { interval: 0 });
    match grpc_client.watch(watch_req).await {
        Ok(_) => (),
        Err(e) => {
            return Err(Box::new(IOError::new(
                ErrorKind::Other,
                format!("failed to watch: error={:?}", e),
            )));
        }
    };

    signal::ctrl_c().await?;
    info!("ctrl-c received");

    // unwatch
    let unwatch_req = Request::new(UnwatchReq {});
    match grpc_client.unwatch(unwatch_req).await {
        Ok(_) => (),
        Err(e) => {
            return Err(Box::new(IOError::new(
                ErrorKind::Other,
                format!("failed to unwatch: error={:?}", e),
            )));
        }
    };

    sleep(Duration::from_secs(1));

    Ok(())
}
