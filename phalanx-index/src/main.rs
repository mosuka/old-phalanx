#[macro_use]
extern crate clap;

use std::io::Error;
use std::net::SocketAddr;

use clap::{App, AppSettings, Arg};
use futures_util::future::join;
use hyper::service::{make_service_fn, service_fn};
use hyper::Server as HyperServer;
use log::*;
use tonic::transport::Server as TonicServer;

use phalanx_common::log::set_logger;
use phalanx_discovery::discovery::etcd::{Etcd, DISCOVERY_TYPE as ETCD_DISCOVERY_TYPE};
use phalanx_discovery::discovery::null::{
    Null as NullDiscovery, DISCOVERY_TYPE as NULL_DISCOVERY_TYPE,
};
use phalanx_discovery::discovery::{Discovery, NodeStatus, State};
use phalanx_index::index::config::{
    IndexConfig, DEFAULT_INDEXER_MEMORY_SIZE, DEFAULT_INDEX_DIRECTORY, DEFAULT_SCHEMA_FILE,
    DEFAULT_TOKENIZER_FILE, DEFAULT_UNIQUE_KEY_FIELD,
};
use phalanx_index::server::grpc::MyIndexService;
use phalanx_index::server::http::handle;
use phalanx_proto::index::index_service_server::IndexServiceServer;
use phalanx_storage::storage::minio::Minio;
use phalanx_storage::storage::minio::STORAGE_TYPE as MINIO_STORAGE_TYPE;
use phalanx_storage::storage::null::Null as NullStorage;
use phalanx_storage::storage::Storage;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    set_logger();

    let default_indexer_threads = format!("{}", num_cpus::get().to_owned());
    let default_indexer_memory_size = format!("{}", DEFAULT_INDEXER_MEMORY_SIZE);

    let app = App::new(crate_name!())
        .setting(AppSettings::DeriveDisplayOrder)
        .version(crate_version!())
        .author(crate_authors!())
        .about("Phalanx server")
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
                .default_value("9000")
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
            Arg::with_name("CLUSTER")
                .help("Cluster name.")
                .long("cluster")
                .value_name("CLUSTER")
                .default_value("default")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("SHARD")
                .help("Shard name.")
                .long("shard")
                .value_name("SHARD")
                .default_value("0")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("NODE")
                .help("Node name.")
                .long("node")
                .value_name("NODE")
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
                .help("The etcd endpoints. Use `,` to separate address. Example: `192.168.1.10:2379,192.168.1.11:2379`")
                .long("etcd-endpoints")
                .value_name("IP:PORT")
                .default_value("127.0.0.1:2379")
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
                .default_value("/phalanx")
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
                .default_value("minioadmin")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("MINIO_SECRET_KEY")
                .help("Secret key for MinIO.")
                .long("minio-secret-key")
                .value_name("MINIO_SECRET_KEY")
                .default_value("minioadmin")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("MINIO_ENDPOINT")
                .help("MinIO endpoint.")
                .long("minio-endpoint")
                .value_name("MINIO_ENDPOINT")
                .default_value("http://127.0.0.1:9000")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("MINIO_BUCKET")
                .help("MinIO bucket.")
                .long("minio-bucket")
                .value_name("MINIO_BUCKET")
                .default_value("phalanx")
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

    let index_directory = matches.value_of("INDEX_DIRECTORY").unwrap();
    let schema_file = matches.value_of("SCHEMA_FILE").unwrap();
    let tokenizer_file = matches.value_of("TOKENIZER_FILE").unwrap();
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
    let unique_key_field = matches.value_of("UNIQUE_ID_FIELD").unwrap();

    let cluster = matches.value_of("CLUSTER").unwrap();
    let shard = matches.value_of("SHARD").unwrap();
    let node = matches.value_of("NODE").unwrap();

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

    let mut index_config = IndexConfig::new();
    index_config.index_dir = String::from(index_directory);
    index_config.schema_file = String::from(schema_file);
    index_config.tokenizer_file = String::from(tokenizer_file);
    index_config.indexer_threads = indexer_threads;
    index_config.indexer_memory_size = indexer_memory_size;
    index_config.unique_key_field = String::from(unique_key_field);

    let storage: Box<dyn Storage> = match storage_type {
        MINIO_STORAGE_TYPE => {
            info!("enable MinIO");
            Box::new(Minio::new(
                minio_access_key,
                minio_secret_key,
                minio_endpoint,
                minio_bucket,
                index_config.index_dir.as_str(),
            ))
        }
        _ => {
            info!("disable object storage");
            Box::new(NullStorage::new())
        }
    };

    let grpc_addr: SocketAddr = format!("{}:{}", host, grpc_port).parse().unwrap();
    let grpc_service = MyIndexService::new(index_config, cluster, shard, storage);

    let grpc_server = TonicServer::builder()
        .add_service(IndexServiceServer::new(grpc_service))
        .serve(grpc_addr);
    info!("start gRPC server on {}", grpc_addr);

    let http_addr: SocketAddr = format!("{}:{}", host, http_port).parse().unwrap();
    let http_service = make_service_fn(|_| async { Ok::<_, Error>(service_fn(handle)) });
    let http_server = HyperServer::bind(&http_addr).serve(http_service);
    info!("start HTTP server on {}", http_addr);

    let mut discovery: Box<dyn Discovery> = match discovery_type {
        ETCD_DISCOVERY_TYPE => {
            info!("enable etcd");
            Box::new(Etcd::new(etcd_endpoints, etcd_root))
        }
        _ => {
            info!("disable node discovery");
            Box::new(NullDiscovery::new())
        }
    };

    // add node to cluster
    if discovery.get_type() != NULL_DISCOVERY_TYPE {
        let node_status = NodeStatus {
            state: State::Active,
            primary: false,
            address: format!("{}:{}", host, grpc_port),
        };
        match discovery.set_node(cluster, shard, node, node_status).await {
            Ok(_) => (),
            Err(e) => return Err(e),
        };
    }

    let _join = join(grpc_server, http_server).await;

    Ok(())
}
