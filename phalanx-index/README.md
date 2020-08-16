# Phalanx

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Phalanx is a cloud-based index writer and searcher written in [Rust](https://www.rust-lang.org/).  
Phalanx makes easy for programmers to develop search applications with advanced features and high availability.

```shell script
$ phalanx-index --host=0.0.0.0 \
                --grpc-port=5000 \
                --http-port=8000 \
                --index-directory=${HOME}/tmp/phalanx0 \
                --schema-file=./examples/schema.json \
                --tokenizer-file=./examples/tokenizer.json \
                --indexer-threads=1 \
                --indexer-memory-size=500000000 \
                --unique-id-field=id \
                --index-name=index0 \
                --shard-name=shard0 \
                --node-name=node0 \
                --discovery-type=etcd \
                --etcd-endpoints=127.0.0.1:2379 \
                --etcd-root=/phalanx \
                --storage-type=minio \
                --minio-access-key=minioadmin \
                --minio-secret-key=minioadmin \
                --minio-endpoint=http://127.0.0.1:9000 \
                --minio-bucket=phalanx
```

```shell script
$ phalanx-index --host=0.0.0.0 \
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
                --etcd-endpoints=127.0.0.1:2379 \
                --etcd-root=/phalanx \
                --storage-type=minio \
                --minio-access-key=minioadmin \
                --minio-secret-key=minioadmin \
                --minio-endpoint=http://127.0.0.1:9000 \
                --minio-bucket=phalanx
```

```shell script
$ phalanx-index --host=0.0.0.0 \
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
                --etcd-endpoints=127.0.0.1:2379 \
                --etcd-root=/phalanx \
                --storage-type=minio \
                --minio-access-key=minioadmin \
                --minio-secret-key=minioadmin \
                --minio-endpoint=http://127.0.0.1:9000 \
                --minio-bucket=phalanx
```

```shell script
$ phalanx-index --host=0.0.0.0 \
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
                --etcd-endpoints=127.0.0.1:2379 \
                --etcd-root=/phalanx \
                --storage-type=minio \
                --minio-access-key=minioadmin \
                --minio-secret-key=minioadmin \
                --minio-endpoint=http://127.0.0.1:9000 \
                --minio-bucket=phalanx
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
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -d "$(jq -c '. | {doc:. | @json}' ./examples/bulk_put.jsonl | jq -s -c '{ requests:.}')" -plaintext 0.0.0.0:5000 phalanx.IndexService/BulkSet
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -d "$(jq -c -s '{ requests:.}' ./examples/bulk_delete.jsonl)" -plaintext 0.0.0.0:5000 phalanx.IndexService/BulkDelete
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -d '{ "query": "rust", "from": 0, "limit": 10, "exclude_count": false, "exclude_docs": false, "facet_field": "category", "facet_prefixes": ["/category/search", "/language"] }' -plaintext 0.0.0.0:5000 phalanx.IndexService/Search
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -plaintext 0.0.0.0:5000 phalanx.IndexService/Push
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -plaintext 0.0.0.0:5000 phalanx.IndexService/Pull
```
