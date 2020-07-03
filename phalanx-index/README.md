# Phalanx

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Phalanx is a cloud-based index writer and searcher written in [Rust](https://www.rust-lang.org/).  
Phalanx makes easy for programmers to develop search applications with advanced features and high availability.

```shell script
$ etcd --data-dir=${HOME}/tmp/etcd
$ minio server ${HOME}/tmp/minio
$ phalanx-index --host=0.0.0.0 \
                --grpc-port=5001 \
                --http-port=9001 \
                --index-directory=${HOME}/tmp/phalanx \
                --schema-file=./examples/schema.json \
                --tokenizer-file=./examples/tokenizer.json \
                --indexer-threads=1 \
                --indexer-memory-size=500000000 \
                --unique-id-field=id \
                --cluster=default \
                --shard=shard0 \
                --node=node0 \
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
$ grpcurl -proto phalanx-proto/proto/index.proto -plaintext 0.0.0.0:5001 phalanx_index.IndexService/Status
$ grpcurl -proto phalanx-proto/proto/index.proto -d '{ "id": "1" }' -plaintext 0.0.0.0:5001 phalanx_index.IndexService/Get
$ grpcurl -proto phalanx-proto/proto/index.proto -d "$(jq -c '. | {doc:@json}' ./examples/doc_1.json)" -plaintext 0.0.0.0:5001 phalanx_index.IndexService/Set
$ grpcurl -proto phalanx-proto/proto/index.proto -d '{ "id": "1" }' -plaintext 0.0.0.0:5001 phalanx_index.IndexService/Delete
$ grpcurl -proto phalanx-proto/proto/index.proto -plaintext 0.0.0.0:5001 phalanx_index.IndexService/Commit
$ grpcurl -proto phalanx-proto/proto/index.proto -plaintext 0.0.0.0:5001 phalanx_index.IndexService/Rollback
$ grpcurl -proto phalanx-proto/proto/index.proto -plaintext 0.0.0.0:5001 phalanx_index.IndexService/Merge
$ grpcurl -proto phalanx-proto/proto/index.proto -plaintext 0.0.0.0:5001 phalanx_index.IndexService/Schema
$ grpcurl -proto phalanx-proto/proto/index.proto -d "$(jq -c '. | {doc:. | @json}' ./examples/bulk_put.jsonl | jq -s -c '{ requests:.}')" -plaintext 0.0.0.0:5001 phalanx_index.IndexService/BulkSet
$ grpcurl -proto phalanx-proto/proto/index.proto -d "$(jq -c -s '{ requests:.}' ./examples/bulk_delete.jsonl)" -plaintext 0.0.0.0:5001 phalanx_index.IndexService/BulkDelete
$ grpcurl -proto phalanx-proto/proto/index.proto -d '{ "query": "rust", "from": 0, "limit": 10, "exclude_count": false, "exclude_docs": false, "facet_field": "category", "facet_prefixes": ["/category/search", "/language"] }' -plaintext 0.0.0.0:5001 phalanx_index.IndexService/Search
$ grpcurl -proto phalanx-proto/proto/index.proto -plaintext 0.0.0.0:5001 phalanx_index.IndexService/Push
$ grpcurl -proto phalanx-proto/proto/index.proto -plaintext 0.0.0.0:5001 phalanx_index.IndexService/Pull
```
