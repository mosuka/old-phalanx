#[macro_use]
extern crate clap;

use std::convert::{Infallible, TryFrom};
use std::io::{Error as IOError, ErrorKind};
use std::net::SocketAddr;
use std::thread::sleep;
use std::time::Duration;

use clap::{App, AppSettings, Arg};
use hyper::service::{make_service_fn, service_fn};
use hyper::Server as HyperServer;
use log::*;
use tokio::signal;
use tonic::transport::Server as TonicServer;
use tonic::Request;

use phalanx_common::log::set_logger;
use phalanx_discovery::discovery::etcd::{Etcd as EtcdDiscovery, TYPE as ETCD_TYPE};
use phalanx_discovery::discovery::nop::{Nop as NopDiscovery, TYPE as NOP_TYPE};
use phalanx_discovery::discovery::Discovery;
use phalanx_overseer::server::grpc::OverseerService;
use phalanx_overseer::server::http::handle;
use phalanx_proto::phalanx::overseer_service_client::OverseerServiceClient;
use phalanx_proto::phalanx::overseer_service_server::OverseerServiceServer;
use phalanx_proto::phalanx::{UnwatchReq, WatchReq};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    set_logger();

    let app = App::new(crate_name!())
        .setting(AppSettings::DeriveDisplayOrder)
        .version(crate_version!())
        .author(crate_authors!())
        .about("Phalanx discovery")
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
                .default_value("5100")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("HTTP_PORT")
                .help("HTTP port number")
                .long("http-port")
                .value_name("HTTP_PORT")
                .default_value("8100")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("DISCOVERY_TYPE")
                .help("Discovery type. Supported: etcd")
                .long("discovery-type")
                .value_name("DISCOVERY_TYPE")
                .default_value("etcd")
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
            Arg::with_name("WATCH_INTERVAL")
                .help("Watch interval (in milliseconds)")
                .long("watch-interval")
                .value_name("WATCH_INTERVAL")
                .default_value("500")
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
    let discovery_type = matches.value_of("DISCOVERY_TYPE").unwrap();

    let etcd_endpoints: Vec<&str> = matches
        .values_of("ETCD_ENDPOINTS")
        .unwrap()
        .map(|addr| addr)
        .collect();
    let etcd_root = matches.value_of("ETCD_ROOT").unwrap();

    let watch_interval = matches
        .value_of("WATCH_INTERVAL")
        .unwrap()
        .parse::<u64>()
        .unwrap();

    let discovery: Box<dyn Discovery> = match discovery_type {
        ETCD_TYPE => {
            info!("enable etcd");
            Box::new(EtcdDiscovery::new(etcd_endpoints, etcd_root))
        }
        _ => {
            info!("disable node discovery");
            Box::new(NopDiscovery::new())
        }
    };
    if discovery.get_type() == NOP_TYPE {
        return Err(Box::try_from(IOError::new(
            ErrorKind::Other,
            format!("unsupported discovery type: {}", discovery_type),
        ))
        .unwrap());
    }

    let grpc_addr: SocketAddr = format!("{}:{}", host, grpc_port).parse().unwrap();
    let grpc_service = OverseerService::new(discovery);
    tokio::spawn(
        TonicServer::builder()
            .add_service(OverseerServiceServer::new(grpc_service))
            .serve(grpc_addr),
    );
    info!("start gRPC server on {}", grpc_addr);

    let grpc_server_url = format!("http://{}:{}", host, grpc_port);
    let mut grpc_client = OverseerServiceClient::connect(grpc_server_url.clone())
        .await
        .unwrap();
    info!("start gRPC client for {}", &grpc_server_url);

    let http_addr: SocketAddr = format!("{}:{}", host, http_port).parse().unwrap();
    let http_service =
        make_service_fn(
            move |_| async move { Ok::<_, Infallible>(service_fn(move |req| handle(req))) },
        );
    tokio::spawn(HyperServer::bind(&http_addr).serve(http_service));
    info!("start HTTP server on {}", http_addr);

    let watch_req = Request::new(WatchReq {
        interval: watch_interval,
    });

    match grpc_client.watch(watch_req).await {
        Ok(_) => (),
        Err(e) => {
            return Err(Box::try_from(IOError::new(
                ErrorKind::Other,
                format!("failed to watch: error={:?}", e),
            ))
            .unwrap())
        }
    };

    signal::ctrl_c().await?;
    info!("ctrl-c received");

    let unwatch_req = Request::new(UnwatchReq {});

    match grpc_client.unwatch(unwatch_req).await {
        Ok(_) => (),
        Err(e) => {
            return Err(Box::try_from(IOError::new(
                ErrorKind::Other,
                format!("failed to unwatch: error={:?}", e),
            ))
            .unwrap())
        }
    };

    sleep(Duration::from_secs(1));

    Ok(())
}
