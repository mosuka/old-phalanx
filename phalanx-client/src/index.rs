use std::convert::TryFrom;
use std::io::{Error, ErrorKind};

use tonic::transport::Channel;

use phalanx_proto::index::index_service_client::IndexServiceClient;
use phalanx_proto::index::StatusReq;

#[derive(Clone)]
pub struct IndexClient {
    client_id: u64,
    client: IndexServiceClient<Channel>,
}

impl IndexClient {
    pub async fn new(addr: &str) -> IndexClient {
        let url = format!("http://{}", addr);
        let client = IndexServiceClient::connect(url).await.unwrap();

        IndexClient {
            client,
            client_id: rand::random(),
        }
    }

    pub async fn status(&mut self) -> Result<String, Box<dyn std::error::Error>> {
        let req = tonic::Request::new(StatusReq {});

        match self.client.status(req).await {
            Ok(resp) => Ok(resp.into_inner().status),
            Err(e) => Err(Box::try_from(Error::new(ErrorKind::Other, format!("{:?}", e))).unwrap()),
        }
    }
}
