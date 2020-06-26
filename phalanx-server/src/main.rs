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
use phalanx_proto::index::index_service_server::IndexServiceServer;
use phalanx_server::index::config::{
    IndexConfig, DEFAULT_INDEXER_MEMORY_SIZE, DEFAULT_INDEX_DIRECTORY, DEFAULT_SCHEMA_FILE,
    DEFAULT_TOKENIZER_FILE, DEFAULT_UNIQUE_KEY_FIELD,
};
use phalanx_server::index::server::MyIndexService;
use phalanx_server::metric::server::handle;
use phalanx_server::storage::minio::{Minio, STORAGE_TYPE};
use phalanx_server::storage::null::Null;
use phalanx_server::storage::Storage;

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
                // .short("H")
                .long("host")
                .value_name("HOST")
                .default_value("0.0.0.0")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("INDEX_PORT")
                .help("Index service port number")
                // .short("i")
                .long("index-port")
                .value_name("INDEX_PORT")
                .default_value("5000")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("METRICS_PORT")
                .help("Metrics service port number")
                // .short("m")
                .long("metrics-port")
                .value_name("METRICS_PORT")
                .default_value("9000")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("INDEX_DIRECTORY")
                .help("Index directory. Stores index, snapshots, and raft logs.")
                // .short("d")
                .long("index-directory")
                .value_name("INDEX_DIRECTORY")
                .default_value(DEFAULT_INDEX_DIRECTORY)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("SCHEMA_FILE")
                .help("Schema file. Must specify An existing file name.")
                // .short("s")
                .long("schema-file")
                .value_name("SCHEMA_FILE")
                .default_value(DEFAULT_SCHEMA_FILE)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("TOKENIZER_FILE")
                .help("Tokenizer file. Must specify An existing file name.")
                // .short("t")
                .long("tokenizer-file")
                .value_name("TOKENIZER_FILE")
                .default_value(DEFAULT_TOKENIZER_FILE)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("INDEXER_THREADS")
                .help("Number of indexer threads. By default indexer uses number of available logical cpu as threads count.")
                // .short("T")
                .long("indexer-threads")
                .value_name("INDEXER_THREADS")
                .default_value(&default_indexer_threads)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("INDEXER_MEMORY_SIZE")
                .help("Total memory size (in bytes) used by the indexer.")
                // .short("M")
                .long("indexer-memory-size")
                .value_name("INDEXER_MEMORY_SIZE")
                .default_value(&default_indexer_memory_size)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("UNIQUE_ID_FIELD")
                .help("Unique id field name.")
                // .short("u")
                .long("unique-id-field")
                .value_name("UNIQUE_ID_FIELD")
                .default_value(DEFAULT_UNIQUE_KEY_FIELD)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("CLUSTER")
                .help("Cluster name.")
                // .short("c")
                .long("cluster")
                .value_name("CLUSTER")
                .default_value("default")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("SHARD")
                .help("Shard name.")
                // .short("S")
                .long("shard")
                .value_name("SHARD")
                .default_value("0")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("STORAGE_TYPE")
                .help("Object storage type. Supported object storage: minio")
                // .short("S")
                .long("storage-type")
                .value_name("STORAGE_TYPE")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("MINIO_ACCESS_KEY")
                .help("Access key for MinIO.")
                // .short("S")
                .long("minio-access-key")
                .value_name("MINIO_ACCESS_KEY")
                .default_value("minioadmin")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("MINIO_SECRET_KEY")
                .help("Secret key for MinIO.")
                // .short("S")
                .long("minio-secret-key")
                .value_name("MINIO_SECRET_KEY")
                .default_value("minioadmin")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("MINIO_ENDPOINT")
                .help("MinIO endpoint.")
                // .short("S")
                .long("minio-endpoint")
                .value_name("MINIO_ENDPOINT")
                .default_value("http://127.0.0.1:9000")
                .takes_value(true),
        );

    let matches = app.get_matches();

    let host = matches.value_of("HOST").unwrap();
    let index_port = matches
        .value_of("INDEX_PORT")
        .unwrap()
        .parse::<u16>()
        .unwrap();
    let metrics_port = matches
        .value_of("METRICS_PORT")
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
    let storage_type = match matches.value_of("STORAGE_TYPE") {
        Some(storage_type) => storage_type,
        _ => "",
    };
    let minio_access_key = matches.value_of("MINIO_ACCESS_KEY").unwrap();
    let minio_secret_key = matches.value_of("MINIO_SECRET_KEY").unwrap();
    let minio_endpoint = matches.value_of("MINIO_ENDPOINT").unwrap();

    let mut index_config = IndexConfig::new();
    index_config.index_dir = String::from(index_directory);
    index_config.schema_file = String::from(schema_file);
    index_config.tokenizer_file = String::from(tokenizer_file);
    index_config.indexer_threads = indexer_threads;
    index_config.indexer_memory_size = indexer_memory_size;
    index_config.unique_key_field = String::from(unique_key_field);

    let storage: Box<dyn Storage> = match storage_type {
        STORAGE_TYPE => {
            info!("enable MinIO");
            Box::new(Minio::new(
                minio_access_key,
                minio_secret_key,
                minio_endpoint,
            ))
        },
        _ => {
            info!("disable object storage");
            Box::new(Null::new())
        },
    };

    let index_addr: SocketAddr = format!("{}:{}", host, index_port).parse().unwrap();
    let index_service = MyIndexService::new(index_config, cluster, shard, storage);
    let index_server = TonicServer::builder()
        .add_service(IndexServiceServer::new(index_service))
        .serve(index_addr);
    info!("index service listening on {}", index_addr);

    let metrics_addr: SocketAddr = format!("{}:{}", host, metrics_port).parse().unwrap();
    let metrics_service = make_service_fn(|_| async { Ok::<_, Error>(service_fn(handle)) });
    let metrics_server = HyperServer::bind(&metrics_addr).serve(metrics_service);
    info!("metric service listening on {}", metrics_addr);

    let _ret = join(index_server, metrics_server).await;

    Ok(())
}
