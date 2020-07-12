use hyper::{Body, Method, Request, Response, StatusCode};
use prometheus::{Encoder, TextEncoder};
use tonic::transport::Channel;

use phalanx_proto::index::index_service_client::IndexServiceClient;
use phalanx_proto::index::StatusReq;

pub async fn handle(
    mut grpc_client: IndexServiceClient<Channel>,
    req: Request<Body>,
) -> Result<Response<Body>, hyper::Error> {
    let mut response = Response::new(Body::empty());
    match (req.method(), req.uri().path()) {
        (&Method::GET, "/metrics") => {
            let metric_families = prometheus::gather();
            let mut buffer = Vec::<u8>::new();
            let encoder = TextEncoder::new();
            encoder.encode(&metric_families, &mut buffer).unwrap();
            let metrics_text = String::from_utf8(buffer.clone()).unwrap();

            *response.status_mut() = StatusCode::OK;
            *response.body_mut() = Body::from(metrics_text);
        }
        (&Method::GET, "/healthz/liveness") => {
            let r = tonic::Request::new(StatusReq {});

            match grpc_client.status(r).await {
                Ok(resp) => {
                    let status = resp.into_inner().status;
                    *response.status_mut() = StatusCode::OK;
                    *response.body_mut() = Body::from(status);
                }
                Err(e) => {
                    let msg = format!("{:?}", e);
                    *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                    *response.body_mut() = Body::from(msg);
                }
            };
        }
        (&Method::GET, "/healthz/readiness") => {
            let r = tonic::Request::new(StatusReq {});

            match grpc_client.status(r).await {
                Ok(resp) => {
                    let status = resp.into_inner().status;
                    *response.status_mut() = StatusCode::OK;
                    *response.body_mut() = Body::from(status);
                }
                Err(e) => {
                    let msg = format!("{:?}", e);
                    *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                    *response.body_mut() = Body::from(msg);
                }
            };
        }
        _ => {
            *response.status_mut() = StatusCode::NOT_FOUND;
        }
    };
    Ok(response)
}
