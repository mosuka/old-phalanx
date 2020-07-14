#[macro_use]
extern crate clap;

use std::io::{Error, ErrorKind};
use std::net::SocketAddr;

use clap::{App, AppSettings, Arg};
use futures_util::future::join;
use hyper::service::{make_service_fn, service_fn};
use hyper::Server as HyperServer;
use log::*;
use tokio::signal;

use phalanx_common::log::set_logger;
use phalanx_discovery::discovery::etcd::{Etcd, DISCOVERY_TYPE as ETCD_DISCOVERY_TYPE};
use phalanx_discovery::discovery::null::{
    Null as NullDiscovery, DISCOVERY_TYPE as NULL_DISCOVERY_TYPE,
};
use phalanx_discovery::discovery::Discovery;
use phalanx_manager::server::http::handle;
use std::convert::TryFrom;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    set_logger();

    let app = App::new(crate_name!())
        .setting(AppSettings::DeriveDisplayOrder)
        .version(crate_version!())
        .author(crate_authors!())
        .about("Phalanx overseer")
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
            Arg::with_name("HTTP_PORT")
                .help("HTTP port number")
                .long("http-port")
                .value_name("HTTP_PORT")
                .default_value("9000")
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
        );

    let matches = app.get_matches();

    let host = matches.value_of("HOST").unwrap();
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
    if discovery.get_type() == NULL_DISCOVERY_TYPE {
        return Err(Box::try_from(Error::new(ErrorKind::Other, format!("unsupported discovery type: {}", discovery_type))).unwrap());
    }

    // match discovery_type {
    //     ETCD_DISCOVERY_TYPE => {
    //         info!("enable etcd");
    //         tokio::spawn(Etcd::new(etcd_endpoints, etcd_root).update_cluster("default"));
    //     }
    //     _ => {
    //         return Err(Box::try_from(Error::new(ErrorKind::Other, format!("unsupported discovery type: {}", discovery_type))).unwrap());
    //     }
    // };

    let http_addr: SocketAddr = format!("{}:{}", host, http_port).parse().unwrap();
    let http_service = make_service_fn(|_| async { Ok::<_, Error>(service_fn(handle)) });
    tokio::spawn(HyperServer::bind(&http_addr).serve(http_service));
    info!("start HTTP server on {}", http_addr);

    // add node to cluster
    if discovery.get_type() != NULL_DISCOVERY_TYPE {
        discovery.update_cluster("default").await;
    }

    // signal::ctrl_c().await?;
    // info!("ctrl-c received");

    Ok(())
}
