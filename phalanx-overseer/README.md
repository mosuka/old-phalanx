# Phalanx

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Phalanx is a cloud-based index writer and searcher written in [Rust](https://www.rust-lang.org/).  
Phalanx makes easy for programmers to develop search applications with advanced features and high availability.

```shell script
$ phalanx-overseer --host=0.0.0.0 \
                   --grpc-port=5100 \
                   --http-port=8100 \
                   --discovery-type=etcd \
                   --etcd-endpoints=http://127.0.0.1:2379 \
                   --etcd-root=/phalanx \
                   --probe-interval=100
```

Invoking RPCs example:

```shell script
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -plaintext 0.0.0.0:5100 phalanx.OverseerService/Readiness
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -d '{ "index_name": "index0", "shard_name": "shard2", "node_name": "node4", "node_details": { "address": "0.0.0.0:5004", "state": 0, "role": 0 } }' -plaintext 0.0.0.0:5100 phalanx.OverseerService/Register
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -d '{ "index_name": "index0", "shard_name": "shard2", "node_name": "node4" }' -plaintext 0.0.0.0:5100 phalanx.OverseerService/Unregister
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -d '{ "index_name": "index0", "shard_name": "shard2", "node_name": "node4" }' -plaintext 0.0.0.0:5100 phalanx.OverseerService/Inquire
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -plaintext 0.0.0.0:5100 phalanx.OverseerService/Watch
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -plaintext 0.0.0.0:5100 phalanx.OverseerService/Unwatch
```
