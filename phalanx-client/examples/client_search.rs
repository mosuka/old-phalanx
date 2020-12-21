use std::fs;
use std::thread::sleep;

use anyhow::{Context, Result};
use tokio::time::Duration;

use phalanx_client::client::Client;
use phalanx_common::log::set_logger;
use phalanx_discovery::discovery::etcd::{Etcd as EtcdDiscovery, EtcdConfig};
use phalanx_discovery::discovery::DiscoveryContainer;

#[tokio::main]
async fn main() -> Result<()> {
    set_logger();

    let discovery_container = DiscoveryContainer {
        discovery: Box::new(EtcdDiscovery::new(EtcdConfig {
            endpoints: vec!["http://localhost:2379".to_string()],
            root: Some("/phalanx".to_string()),
            auth: None,
            tls_ca_path: None,
            tls_cert_path: None,
            tls_key_path: None,
        })),
    };

    let mut client = Client::new(discovery_container).await;

    client
        .watch("/")
        .await
        .with_context(|| "failed to watch key that starts with \"/\"")?;

    sleep(Duration::from_secs(1));

    let search_request = fs::read("./examples/search_request.json")
        .with_context(|| "failed to read document JSON")?;

    match client.search(search_request, "index0").await {
        Ok(doc) => {
            println!("{}", String::from_utf8(doc).unwrap());
        }
        Err(e) => {
            println!("error: {}", e.to_string());
        }
    };

    match client.unwatch().await {
        Ok(_) => (),
        Err(e) => println!("error: {}", e.to_string()),
    };

    sleep(Duration::from_secs(1));

    Ok(())
}
