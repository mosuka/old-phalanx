# Phalanx

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Phalanx is a cloud-based index writer and searcher written in [Rust](https://www.rust-lang.org/).  
Phalanx makes easy for programmers to develop search applications with advanced features and high availability.

```shell script
$ phalanx overseer --address=0.0.0.0 \
                   --grpc-port=5100 \
                   --http-port=8100 \
                   --discovery-type=etcd \
                   --discovery-root=/phalanx \
                   --etcd-endpoints=http://127.0.0.1:2379 \
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


```shell script
$ phalanx index --address=0.0.0.0 \
                --grpc-port=5000 \
                --http-port=8000 \
                --index-directory=${HOME}/tmp/phalanx0 \
                --schema-file=./etc/schema.json \
                --tokenizer-file=./etc/tokenizer.json \
                --indexer-threads=1 \
                --indexer-memory-size=500000000 \
                --unique-id-field=id \
                --index-name=index0 \
                --shard-name=shard0 \
                --node-name=node0 \
                --discovery-type=etcd \
                --discovery-root=/phalanx \
                --etcd-endpoints=http://127.0.0.1:2379 \
                --storage-type=minio \
                --storage-bucket=phalanx \
                --minio-access-key=minioadmin \
                --minio-secret-key=minioadmin \
                --minio-endpoint=http://127.0.0.1:9000
```

```shell script
$ phalanx index --address=0.0.0.0 \
                --grpc-port=5001 \
                --http-port=8001 \
                --index-directory=${HOME}/tmp/phalanx1 \
                --schema-file=./examples/schema.json \
                --tokenizer-file=./examples/tokenizer.json \
                --indexer-threads=1 \
                --indexer-memory-size=500000000 \
                --unique-id-field=id \
                --index-name=index0 \
                --shard-name=shard0 \
                --node-name=node1 \
                --discovery-type=etcd \
                --discovery-root=/phalanx \
                --etcd-endpoints=http://127.0.0.1:2379 \
                --storage-type=minio \
                --storage-bucket=phalanx \
                --minio-access-key=minioadmin \
                --minio-secret-key=minioadmin \
                --minio-endpoint=http://127.0.0.1:9000
```

```shell script
$ phalanx index --address=0.0.0.0 \
                --grpc-port=5002 \
                --http-port=8002 \
                --index-directory=${HOME}/tmp/phalanx2 \
                --schema-file=./examples/schema.json \
                --tokenizer-file=./examples/tokenizer.json \
                --indexer-threads=1 \
                --indexer-memory-size=500000000 \
                --unique-id-field=id \
                --index-name=index0 \
                --shard-name=shard1 \
                --node-name=node2 \
                --discovery-type=etcd \
                --discovery-root=/phalanx \
                --etcd-endpoints=http://127.0.0.1:2379 \
                --storage-type=minio \
                --storage-bucket=phalanx \
                --minio-access-key=minioadmin \
                --minio-secret-key=minioadmin \
                --minio-endpoint=http://127.0.0.1:9000
```

```shell script
$ phalanx index --address=0.0.0.0 \
                --grpc-port=5003 \
                --http-port=8003 \
                --index-directory=${HOME}/tmp/phalanx3 \
                --schema-file=./examples/schema.json \
                --tokenizer-file=./examples/tokenizer.json \
                --indexer-threads=1 \
                --indexer-memory-size=500000000 \
                --unique-id-field=id \
                --index-name=index0 \
                --shard-name=shard1 \
                --node-name=node3 \
                --discovery-type=etcd \
                --discovery-root=/phalanx \
                --etcd-endpoints=http://127.0.0.1:2379 \
                --storage-type=minio \
                --storage-bucket=phalanx \
                --minio-access-key=minioadmin \
                --minio-secret-key=minioadmin \
                --minio-endpoint=http://127.0.0.1:9000
```

Invoking RPCs example:

```shell script
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -plaintext 0.0.0.0:5000 phalanx.IndexService/Readiness
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -d '{ "id": "1" }' -plaintext 0.0.0.0:5000 phalanx.IndexService/Get
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -d "$(jq -c '. | {doc:@json}' ./examples/doc_1.json)" -plaintext 0.0.0.0:5000 phalanx.IndexService/Set
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -d '{ "id": "1" }' -plaintext 0.0.0.0:5000 phalanx.IndexService/Delete
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -plaintext 0.0.0.0:5000 phalanx.IndexService/Commit
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -plaintext 0.0.0.0:5000 phalanx.IndexService/Rollback
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -plaintext 0.0.0.0:5000 phalanx.IndexService/Merge
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -plaintext 0.0.0.0:5000 phalanx.IndexService/Schema
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -d "$(jq -c '. | @json' ./examples/bulk_put.jsonl | jq -s -c '{ docs:. }')" -plaintext 0.0.0.0:5000 phalanx.IndexService/BulkSet
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -d "$(jq '. | @json' ./examples/bulk_delete.txt  | jq -s -c '{ ids:. }')" -plaintext 0.0.0.0:5000 phalanx.IndexService/BulkDelete
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -d "$(jq '. | @json' ./examples/search_request.json  | jq -s -c '{ request:. }')" -plaintext 0.0.0.0:5000 phalanx.IndexService/Search | jq -r '.result' | jq .
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -plaintext 0.0.0.0:5000 phalanx.IndexService/Push
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -plaintext 0.0.0.0:5001 phalanx.IndexService/Pull
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -plaintext 0.0.0.0:5002 phalanx.IndexService/Pull
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -plaintext 0.0.0.0:5003 phalanx.IndexService/Pull
```
