use std::convert::TryFrom;
use std::io::{Error, ErrorKind};

use phalanx_client::index::IndexClient;
use phalanx_common::log::set_logger;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    set_logger();

    let addr = "0.0.0.0:5001";

    let mut client = IndexClient::new(addr).await;

    match client.status().await {
        Ok(status) => {
            println!("{:?}", status);
            Ok(())
        }
        Err(e) => Err(Box::try_from(Error::new(ErrorKind::Other, format!("{:?}", e))).unwrap()),
    }
}
