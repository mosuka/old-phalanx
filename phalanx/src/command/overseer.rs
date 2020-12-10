use std::convert::Infallible;
use std::net::ToSocketAddrs;
use std::thread::sleep;
use std::time::Duration;

use anyhow::{anyhow, Result};
use clap::ArgMatches;
use hyper::service::{make_service_fn, service_fn};
use hyper::Server as HyperServer;
use log::*;
use tokio::signal;
use tonic::transport::Server as TonicServer;
use tonic::Request;

use phalanx_common::log::set_logger;
use phalanx_discovery::discovery::etcd::{
    Etcd as EtcdDiscovery, EtcdConfig, TYPE as ETCD_DISCOVERY_TYPE,
};
use phalanx_discovery::discovery::nop::{Nop as NopDiscovery, TYPE as NOP_DISCOVERY_TYPE};
use phalanx_discovery::discovery::DiscoveryContainer;
use phalanx_overseer::overseer::Overseer;
use phalanx_overseer::server::grpc::OverseerService;
use phalanx_overseer::server::http::handle;
use phalanx_proto::phalanx::overseer_service_client::OverseerServiceClient;
use phalanx_proto::phalanx::overseer_service_server::OverseerServiceServer;
use phalanx_proto::phalanx::{UnwatchReq, WatchReq};

pub async fn run_overseer(matches: &ArgMatches<'_>) -> Result<()> {
    set_logger();

    let discovery_container = match matches.value_of("discovery_type") {
        Some(discovery_type) => {
            let discovery_root = match matches.value_of("discovery_root") {
                Some(discovery_root) => discovery_root,
                None => {
                    return Err(anyhow!("missing --discovery-root"));
                }
            };

            match discovery_type {
                ETCD_DISCOVERY_TYPE => {
                    let etcd_endpoints: Vec<String> = match matches.values_of("etcd_endpoints") {
                        Some(etcd_endpoints) => {
                            etcd_endpoints.map(|addr| addr.to_string()).collect()
                        }
                        None => {
                            return Err(anyhow!("missing --etcd-endpoints"));
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
                    return Err(anyhow!("unsupported discovery type: {}", discovery_type));
                }
            }
        }
        None => DiscoveryContainer {
            discovery: Box::new(NopDiscovery::new()),
        },
    };

    let probe_interval = match matches.value_of("probe_interval") {
        Some(probe_interval) => match probe_interval.parse::<u64>() {
            Ok(probe_interval) => probe_interval,
            Err(e) => {
                return Err(anyhow!(
                    "failed to parse gRPC port {}: {}",
                    probe_interval,
                    e.to_string()
                ));
            }
        },
        None => {
            return Err(anyhow!("missing --probe-interval"));
        }
    };

    let overseer = Overseer::new(discovery_container);

    let address = match matches.value_of("address") {
        Some(host) => host,
        None => return Err(anyhow!("missing --address")),
    };

    let grpc_port = match matches.value_of("grpc_port") {
        Some(grpc_port) => match grpc_port.parse::<u16>() {
            Ok(grpc_port) => grpc_port,
            Err(e) => {
                return Err(anyhow!(
                    "failed to parse gRPC port {}: {}",
                    grpc_port,
                    e.to_string()
                ));
            }
        },
        None => {
            return Err(anyhow!("missing --grpc-port"));
        }
    };

    let http_port = match matches.value_of("http_port") {
        Some(http_port) => match http_port.parse::<u16>() {
            Ok(http_port) => http_port,
            Err(e) => {
                return Err(anyhow!(
                    "failed to parse HTTP port {}: {}",
                    http_port,
                    e.to_string()
                ));
            }
        },
        None => {
            return Err(anyhow!("missing --http-port"));
        }
    };

    // resolve gRPC address
    let grpc_address = format!("{}:{}", address, grpc_port);
    let grpc_address = match grpc_address.to_socket_addrs() {
        Ok(mut grpc_sock_addrs) => match grpc_sock_addrs.next() {
            Some(grpc_sock_addr) => grpc_sock_addr,
            None => {
                return Err(anyhow!("failed to resolve address {:?}", grpc_sock_addrs));
            }
        },
        Err(e) => {
            return Err(anyhow!(
                "failed to convert socket address {}: {}",
                grpc_address,
                e.to_string()
            ));
        }
    };

    // resolve HTTP address
    let http_address = format!("{}:{}", address, http_port);
    let http_address = match http_address.to_socket_addrs() {
        Ok(mut http_sock_addrs) => match http_sock_addrs.next() {
            Some(http_sock_addr) => http_sock_addr,
            None => {
                return Err(anyhow!("failed to resolve address {:?}", http_sock_addrs));
            }
        },
        Err(e) => {
            return Err(anyhow!(
                "failed to convert socket address {}: {}",
                http_address,
                e.to_string()
            ));
        }
    };

    // start gRPC server
    let overseer_service = OverseerService::new(overseer);
    tokio::spawn(
        TonicServer::builder()
            .add_service(OverseerServiceServer::new(overseer_service))
            .serve(grpc_address),
    );
    info!("start gRPC server on {}", grpc_address.to_string());

    // start HTTP server
    let http_service =
        make_service_fn(
            move |_| async move { Ok::<_, Infallible>(service_fn(move |req| handle(req))) },
        );
    tokio::spawn(HyperServer::bind(&http_address).serve(http_service));
    info!("start HTTP server on {}", http_address.to_string());

    // create gRPC client
    let mut grpc_client = match OverseerServiceClient::connect(format!(
        "http://{}",
        grpc_address.to_string()
    ))
    .await
    {
        Ok(grpc_client) => grpc_client,
        Err(e) => {
            return Err(anyhow!(
                "failed to connect gRPC server {}: {}",
                grpc_address.to_string(),
                e.to_string()
            ));
        }
    };

    // watch
    let watch_req = Request::new(WatchReq {
        interval: probe_interval,
    });
    match grpc_client.watch(watch_req).await {
        Ok(_) => (),
        Err(e) => {
            return Err(anyhow!(
                "failed to watch cluster {}: {}",
                grpc_address.to_string(),
                e.to_string()
            ));
        }
    };

    signal::ctrl_c().await.unwrap();
    info!("ctrl-c received");

    // unwatch
    let unwatch_req = Request::new(UnwatchReq {});
    match grpc_client.unwatch(unwatch_req).await {
        Ok(_) => (),
        Err(e) => {
            return Err(anyhow!(
                "failed to unwatch cluster {}: {}",
                grpc_address.to_string(),
                e.to_string()
            ));
        }
    };

    sleep(Duration::from_secs(1));

    Ok(())
}
