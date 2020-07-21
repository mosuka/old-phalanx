use std::io::{Error, ErrorKind};

use tonic::transport::Channel;

use phalanx_proto::index::index_service_client::IndexServiceClient;
use phalanx_proto::index::{HealthReq, State};

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

    pub async fn status(&mut self) -> Result<State, Box<dyn std::error::Error>> {
        let req = tonic::Request::new(HealthReq {});

        match self.client.health(req).await {
            Ok(resp) => {
                let state = resp.into_inner().state;
                let s = match state {
                    state if state == State::Ready as i32 => State::Ready,
                    state if state == State::NotReady as i32 => State::NotReady,
                    _ => State::NotReady,
                };

                Ok(s)
            }
            Err(e) => Err(Box::new(Error::new(ErrorKind::Other, format!("{:?}", e)))),
        }
    }
}
