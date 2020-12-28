use std::io::Write;

use anyhow::Result;
use clap::{
    crate_authors, crate_description, crate_name, crate_version, App, AppSettings, Arg, SubCommand,
};

use phalanx::command::dispatcher::run_dispatcher;
use phalanx::command::index::run_index;
use phalanx::command::overseer::run_overseer;
use phalanx_kvs::kvs::etcd::DEFAULT_ENDPOINTS as ETCD_DEFAULT_ENDPOINTS;
use phalanx_kvs::kvs::DEFAULT_ROOT;
use phalanx_index::index::config::{
    DEFAULT_INDEXER_MEMORY_SIZE, DEFAULT_INDEX_DIRECTORY, DEFAULT_SCHEMA_FILE,
    DEFAULT_TOKENIZER_FILE, DEFAULT_UNIQUE_KEY_FIELD,
};
use phalanx_storage::storage::minio::{
    DEFAULT_ACCESS_KEY as MINIO_DEFAULT_ACCESS_KEY, DEFAULT_ENDPOINT as MINIO_DEFAULT_ENDPOINT,
    DEFAULT_SECRET_KEY as MINIO_DEFAULT_SECRET_KEY,
};
use phalanx_storage::storage::DEFAULT_BUCKET;

#[tokio::main]
async fn main() -> Result<()> {
    let indexer_threads = &*num_cpus::get().to_string();
    let indexer_memory_size = &*DEFAULT_INDEXER_MEMORY_SIZE.to_string();

    let app = App::new(crate_name!())
        .setting(AppSettings::DeriveDisplayOrder)
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .setting(AppSettings::DisableHelpSubcommand)
        .setting(AppSettings::DisableHelpSubcommand)
        .version(crate_version!())
        .author(crate_authors!())
        .about(crate_description!())
        .help_message("Prints help information.")
        .version_message("Prints version information.")
        .version_short("v")
        .subcommand(
            SubCommand::with_name("index")
                .setting(AppSettings::DeriveDisplayOrder)
                .setting(AppSettings::DisableVersion)
                .author(crate_authors!())
                .about("Phalanx Index Node")
                .arg(
                    Arg::with_name("address")
                        .help("The hostname, IP or FQDN that runs the index service.")
                        .long("address")
                        .value_name("ADDRESS")
                        .default_value("0.0.0.0")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("grpc_port")
                        .help("The gRPC port number used for the index service.")
                        .long("grpc-port")
                        .value_name("GRPC_PORT")
                        .default_value("5000")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("http_port")
                        .help("The HTTP port number used for the index service.")
                        .long("http-port")
                        .value_name("HTTP_PORT")
                        .default_value("8000")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("index_directory")
                        .help("The directory where the index is stored.")
                        .long("index-directory")
                        .value_name("INDEX_DIRECTORY")
                        .default_value(DEFAULT_INDEX_DIRECTORY)
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("schema_file")
                        .help("A file that describes the schema definition in JSON format.")
                        .long("schema-file")
                        .value_name("SCHEMA_FILE")
                        .default_value(DEFAULT_SCHEMA_FILE)
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("unique_id_field")
                        .help("The field name that stores the value that makes the document unique.")
                        .long("unique-id-field")
                        .value_name("UNIQUE_ID_FIELD")
                        .default_value(DEFAULT_UNIQUE_KEY_FIELD)
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("tokenizer_file")
                        .help("A file that describes the tokenizer definition in JSON format.")
                        .long("tokenizer-file")
                        .value_name("TOKENIZER_FILE")
                        .default_value(DEFAULT_TOKENIZER_FILE)
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("indexer_threads")
                        .help("The number of the indexer threads. The default is the number of available logical CPUs.")
                        .long("indexer-threads")
                        .value_name("INDEXER_THREADS")
                        .default_value(indexer_threads)
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("indexer_memory_size")
                        .help("Total memory size (in bytes) used by the indexer.")
                        .long("indexer-memory-size")
                        .value_name("INDEXER_MEMORY_SIZE")
                        .default_value(indexer_memory_size)
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("index_name")
                        .help("A logical index name consisting of multiple shards.")
                        .long("index-name")
                        .value_name("INDEX_NAME")
                        .default_value("index0")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("shard_name")
                        .help("The name of the partition that constitutes the logical index.")
                        .long("shard-name")
                        .value_name("SHARD_NAME")
                        .default_value("shard0")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("node_name")
                        .help("A unique name for a node in a cluster.")
                        .long("node-name")
                        .value_name("NODE_NAME")
                        .default_value("node0")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("discovery_type")
                        .help("Specify the backend to be used for service discovery. If omitted, service discovery is not used. The available backends are as follows: etcd")
                        .long("discovery-type")
                        .value_name("DISCOVERY_TYPE")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("discovery_root")
                        .help("Specifies the prefix to store the data for the service discovery backend.")
                        .long("discovery-root")
                        .value_name("DISCOVERY_ROOT")
                        .default_value(DEFAULT_ROOT)
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("etcd_endpoints")
                        .help("The etcd endpoints. Use `,` to separate address. Example: `http://etcd1:2379,http://etcd2:2379,http://etcd3:2379`. Enabled when specifying the discovery type as etcd.")
                        .long("etcd-endpoints")
                        .value_name("ETCD_ENDPOINTS")
                        .default_value(ETCD_DEFAULT_ENDPOINTS)
                        .multiple(true)
                        .use_delimiter(true)
                        .require_delimiter(true)
                        .value_delimiter(",")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("storage_type")
                        .help("Specifies the backend to use for object storage. If omitted, object storage is not used and index is stored only on the local disk. The available backends are: minio")
                        .long("storage-type")
                        .value_name("STORAGE_TYPE")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("storage_bucket")
                        .help("Specifies the bucket to store the index for the object storage backend.")
                        .long("storage-bucket")
                        .value_name("STORAGE_BUCKET")
                        .default_value(DEFAULT_BUCKET)
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("minio_access_key")
                        .help("Access key for MinIO. Enabled when specifying the object storage type as MinIO.")
                        .long("minio-access-key")
                        .value_name("MINIO_ACCESS_KEY")
                        .default_value(MINIO_DEFAULT_ACCESS_KEY)
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("minio_secret_key")
                        .help("Secret key for MinIO. Enabled when specifying the object storage type as MinIO.")
                        .long("minio-secret-key")
                        .value_name("MINIO_SECRET_KEY")
                        .default_value(MINIO_DEFAULT_SECRET_KEY)
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("minio_endpoint")
                        .help("MinIO endpoint. Enabled when specifying the object storage type as MinIO.")
                        .long("minio-endpoint")
                        .value_name("MINIO_ENDPOINT")
                        .default_value(MINIO_DEFAULT_ENDPOINT)
                        .takes_value(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("overseer")
                .setting(AppSettings::DeriveDisplayOrder)
                .setting(AppSettings::DisableVersion)
                .author(crate_authors!())
                .about("Runs discovery node")
                .arg(
                    Arg::with_name("address")
                        .help("The hostname, IP or FQDN that runs the index service.")
                        .long("address")
                        .value_name("ADDRESS")
                        .default_value("0.0.0.0")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("grpc_port")
                        .help("The gRPC port number used for the index service.")
                        .long("grpc-port")
                        .value_name("GRPC_PORT")
                        .default_value("5100")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("http_port")
                        .help("The HTTP port number used for the index service.")
                        .long("http-port")
                        .value_name("HTTP_PORT")
                        .default_value("8100")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("discovery_type")
                        .help("Specify the backend to be used for service discovery. If omitted, service discovery is not used. The available backends are as follows: etcd")
                        .long("discovery-type")
                        .value_name("DISCOVERY_TYPE")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("discovery_root")
                        .help("Specifies the prefix to store the data for the service discovery backend.")
                        .long("discovery-root")
                        .value_name("DISCOVERY_ROOT")
                        .default_value(DEFAULT_ROOT)
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("etcd_endpoints")
                        .help("The etcd endpoints. Use `,` to separate address. Example: `http://etcd1:2379,http://etcd2:2379,http://etcd3:2379`. Enabled when specifying the discovery type as etcd.")
                        .long("etcd-endpoints")
                        .value_name("ETCD_ENDPOINTS")
                        .default_value(ETCD_DEFAULT_ENDPOINTS)
                        .multiple(true)
                        .use_delimiter(true)
                        .require_delimiter(true)
                        .value_delimiter(",")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("probe_interval")
                        .help("Specify the interval to probe a node. (in milliseconds)")
                        .long("probe-interval")
                        .value_name("PROBE_INTERVAL")
                        .default_value("100")
                        .takes_value(true),
                ),
        )
        .subcommand(
            SubCommand::with_name("dispatcher")
                .setting(AppSettings::DeriveDisplayOrder)
                .setting(AppSettings::DisableVersion)
                .author(crate_authors!())
                .about("Runs dicpatcher node")
                .arg(
                    Arg::with_name("address")
                        .help("The hostname, IP or FQDN that runs the index service.")
                        .long("address")
                        .value_name("ADDRESS")
                        .default_value("0.0.0.0")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("grpc_port")
                        .help("The gRPC port number used for the index service.")
                        .long("grpc-port")
                        .value_name("GRPC_PORT")
                        .default_value("5200")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("http_port")
                        .help("The HTTP port number used for the index service.")
                        .long("http-port")
                        .value_name("HTTP_PORT")
                        .default_value("8200")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("discovery_type")
                        .help("Specify the backend to be used for service discovery. If omitted, service discovery is not used. The available backends are as follows: etcd")
                        .long("discovery-type")
                        .value_name("DISCOVERY_TYPE")
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("discovery_root")
                        .help("Specifies the prefix to store the data for the service discovery backend.")
                        .long("discovery-root")
                        .value_name("DISCOVERY_ROOT")
                        .default_value(DEFAULT_ROOT)
                        .takes_value(true),
                )
                .arg(
                    Arg::with_name("etcd_endpoints")
                        .help("The etcd endpoints. Use `,` to separate address. Example: `http://etcd1:2379,http://etcd2:2379,http://etcd3:2379`. Enabled when specifying the discovery type as etcd.")
                        .long("etcd-endpoints")
                        .value_name("ETCD_ENDPOINTS")
                        .default_value(ETCD_DEFAULT_ENDPOINTS)
                        .multiple(true)
                        .use_delimiter(true)
                        .require_delimiter(true)
                        .value_delimiter(",")
                        .takes_value(true),
                ),
        );

    let arg_matches = app.get_matches();

    let (subcommand, options) = arg_matches.subcommand();
    let options = options.unwrap();
    let result = match subcommand {
        "index" => run_index(options).await,
        "overseer" => run_overseer(options).await,
        "dispatcher" => run_dispatcher(options).await,
        _ => panic!("unknown command: {:?}", subcommand),
    };
    match result {
        Ok(ret) => Ok(ret),
        Err(e) => {
            let stderr = &mut std::io::stderr();
            let errmsg = "Error writing to stderr";
            writeln!(stderr, "{}", e.to_string()).expect(errmsg);
            std::process::exit(1);
        }
    }
}
