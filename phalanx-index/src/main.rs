#[macro_use]
extern crate clap;

use std::convert::Infallible;
use std::net::SocketAddr;

use clap::{App, AppSettings, Arg};
use hyper::service::{make_service_fn, service_fn};
use hyper::Server as HyperServer;
use log::*;
use tokio::signal;
use tonic::transport::Server as TonicServer;

use phalanx_common::log::set_logger;
use phalanx_discovery::discovery::etcd::{Etcd, TYPE as ETCD_DISCOVERY_TYPE};
use phalanx_discovery::discovery::nop::Nop as NopDiscovery;
use phalanx_discovery::discovery::Discovery;
use phalanx_index::index::config::{
    IndexConfig, DEFAULT_INDEXER_MEMORY_SIZE, DEFAULT_INDEX_DIRECTORY, DEFAULT_SCHEMA_FILE,
    DEFAULT_TOKENIZER_FILE, DEFAULT_UNIQUE_KEY_FIELD,
};
use phalanx_index::server::grpc::IndexService;
use phalanx_index::server::http::handle;
use phalanx_proto::phalanx::index_service_client::IndexServiceClient;
use phalanx_proto::phalanx::index_service_server::IndexServiceServer;
use phalanx_proto::phalanx::{NodeDetails, Role, State};
use phalanx_storage::storage::minio::Minio;
use phalanx_storage::storage::minio::TYPE as MINIO_STORAGE_TYPE;
use phalanx_storage::storage::nop::Nop as NopStorage;
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

    let storage: Box<dyn Storage> = match storage_type {
        MINIO_STORAGE_TYPE => Box::new(Minio::new(
            minio_access_key,
            minio_secret_key,
            minio_endpoint,
            minio_bucket,
            index_directory,
        )),
        _ => Box::new(NopStorage::new()),
    };

    let mut discovery: Box<dyn Discovery> = match discovery_type {
        ETCD_DISCOVERY_TYPE => Box::new(Etcd::new(etcd_endpoints, etcd_root)),
        _ => Box::new(NopDiscovery::new()),
    };

    let mut index_config = IndexConfig::new();
    index_config.index_dir = String::from(index_directory);
    index_config.schema_file = String::from(schema_file);
    index_config.tokenizer_file = String::from(tokenizer_file);
    index_config.indexer_threads = indexer_threads;
    index_config.indexer_memory_size = indexer_memory_size;
    index_config.unique_key_field = String::from(unique_key_field);

    let grpc_addr: SocketAddr = format!("{}:{}", host, grpc_port).parse().unwrap();
    let grpc_service = IndexService::new(index_config, index_name, shard_name, storage);
    tokio::spawn(
        TonicServer::builder()
            .add_service(IndexServiceServer::new(grpc_service))
            .serve(grpc_addr),
    );
    info!("start gRPC server on {}", grpc_addr);

    let grpc_server_url = format!("http://{}:{}", host, grpc_port);
    let grpc_client = IndexServiceClient::connect(grpc_server_url.clone())
        .await
        .unwrap();
    info!("start gRPC client for {}", &grpc_server_url);

    let http_addr: SocketAddr = format!("{}:{}", host, http_port).parse().unwrap();
    let http_service = make_service_fn(move |_| {
        let grpc_client = grpc_client.clone();
        async move { Ok::<_, Infallible>(service_fn(move |req| handle(grpc_client.clone(), req))) }
    });
    tokio::spawn(HyperServer::bind(&http_addr).serve(http_service));
    info!("start HTTP server on {}", http_addr);

    // add node to cluster
    match discovery.get_node(index_name, shard_name, node_name).await {
        Ok(result) => {
            match result {
                Some(node_details) => {
                    // node exists
                    info!(
                            "node is already registered: index_name={:?}, shard_name={:?}, node_name={:?}, node_details={:?}",
                            &index_name, shard_name, node_name, &node_details
                        );
                }
                None => {
                    // node does not exist
                    let node_details = NodeDetails {
                        address: format!("{}:{}", host, grpc_port),
                        state: State::NotReady as i32,
                        role: Role::Candidate as i32,
                    };
                    match discovery
                        .set_node(index_name, shard_name, node_name, node_details)
                        .await
                    {
                        Ok(_) => (),
                        Err(e) => return Err(e),
                    };
                }
            };
        }
        Err(e) => return Err(e),
    };

    signal::ctrl_c().await?;
    info!("ctrl-c received");

    Ok(())
}
