# Phalanx

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Phalanx is a cloud-based index writer and searcher written in [Rust](https://www.rust-lang.org/).  
Phalanx makes easy for programmers to develop search applications with advanced features and high availability.

## Getting started

### Start in standalone mode

Running node in standalone mode is easy. You can start server with the following command:

```shell script
$ phalanx index --index-directory=/tmp/phalanx \
                --schema-file=./etc/schema.json \
                --tokenizer-file=./etc/tokenizer.json
```

#### Get schema

Currently it only supports gRPC. Please connect using the gRPC client as follows.
You can confirm current schema with the following command:

```shell script
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -plaintext 0.0.0.0:5000 phalanx.IndexService/Schema | jq -r '.schema' | jq .
```

#### Index document

You can index document with the following command:

```shell script
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -d "$(jq -c '. | {doc:@json}' ./examples/doc_1.json)" -plaintext 0.0.0.0:5000 phalanx.IndexService/Set
```

Then commit index with the following command:

```shell script
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -plaintext 0.0.0.0:5000 phalanx.IndexService/Commit
```

#### Get a document

You can get document with the following command:

```shell script
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -d '{ "id": "1" }' -plaintext 0.0.0.0:5000 phalanx.IndexService/Get | jq -r '.doc' | jq .
```

You'll see the result in JSON format. The result of the above command is:

```json
{
  "category": [
    "/category/search/server",
    "/language/rust"
  ],
  "description": [
    "Phalanx is a cloud-based index writer and searcher."
  ],
  "id": [
    "1"
  ],
  "name": [
    "Phalanx"
  ],
  "popularity": [
    0
  ],
  "publish_date": [
    "2020-06-21T01:41:00+00:00"
  ],
  "url": [
    "https://github.com/mosuka/phalanx"
  ]
}
```

#### Index documents in bulk

You can index documents in bulk with the following command:

```shell script
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -d "$(jq -c '. | @json' ./examples/bulk_put.jsonl | jq -s -c '{ docs:. }')" -plaintext 0.0.0.0:5000 phalanx.IndexService/BulkSet
```

Then commit index with the following command:

```shell script
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -plaintext 0.0.0.0:5000 phalanx.IndexService/Commit
```

#### Search documents

You can search documents with the following command:

```shell script
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -d "$(jq '. | @json' ./examples/search_request.json  | jq -s -c '{ request:. }')" -plaintext 0.0.0.0:5000 phalanx.IndexService/Search | jq -r '.result' | jq .
```

You'll see the result in JSON format. The result of the above command is:

```json
{
  "count": 2,
  "docs": [
    {
      "fields": {
        "category": [
          "/category/search/server",
          "/language/rust"
        ],
        "description": [
          "Phalanx is a cloud-based index writer and searcher written in Rust."
        ],
        "id": [
          "1"
        ],
        "name": [
          "Phalanx"
        ],
        "popularity": [
          0
        ],
        "publish_date": [
          "2020-06-21T01:41:00+00:00"
        ],
        "url": [
          "https://github.com/mosuka/phalanx"
        ]
      },
      "score": 1.7752718
    },
    {
      "fields": {
        "category": [
          "/category/search/library",
          "/language/rust"
        ],
        "description": [
          "Tantivy is a full-text search engine library inspired by Apache Lucene and written in Rust."
        ],
        "id": [
          "8"
        ],
        "name": [
          "Tantivy"
        ],
        "popularity": [
          3142
        ],
        "publish_date": [
          "2019-12-19T01:07:00+00:00"
        ],
        "url": [
          "https://github.com/tantivy-search/tantivy"
        ]
      },
      "score": 1.57912
    }
  ],
  "facet": {
    "category": {
      "/language/rust": 2,
      "/category/search/server": 1,
      "/category/search/library": 1
    }
  }
}
```

#### Delete document

You can delete document with the following command:

```shell script
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -d '{ "id": "1" }' -plaintext 0.0.0.0:5000 phalanx.IndexService/Delete
```

Then commit index with the following command:

```shell script
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -plaintext 0.0.0.0:5000 phalanx.IndexService/Commit
```

#### Delete documents in bulk

You can delete documents in bulk with the following command:

```shell script
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -d "$(jq '. | @json' ./examples/bulk_delete.txt  | jq -s -c '{ ids:. }')" -plaintext 0.0.0.0:5000 phalanx.IndexService/BulkDelete
```

Then commit index with the following command:

```shell script
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -plaintext 0.0.0.0:5000 phalanx.IndexService/Commit
```


### Start in standalone mode

Phalanx in cluster mode requires etcd for service discovery and MinIO for remote index storage.
Dependent components for testing must be started as following:

```shell script
$ docker-compose up etcd etcdkeeper minio
```

#### Start overseer node

The overseer node probes all nodes in the cluster and updates their roles.
You can start overseer node with the following command:

```shell script
$ phalanx overseer --address=0.0.0.0 \
                   --grpc-port=5100 \
                   --http-port=8100 \
                   --discovery-type=etcd \
                   --discovery-root=/phalanx \
                   --etcd-endpoints=http://127.0.0.1:2379 \
                   --probe-interval=100
```

#### Start index node 

You can start index nodes with the following command:

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
                --schema-file=./etc/schema.json \
                --tokenizer-file=./etc/tokenizer.json \
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

You can see the cluster information by opening a following URL in your browser and connecting to `etcd:2379`:

http://localhost:8080/etcdkeeper/

If you run the above commands, `/phalanx/index0/shard0/node0.json` will be as follows:

```json
{"address":"0.0.0.0:5000","state":2,"role":2}
```

above JSON indicates that it is the primary index node.

#### Index documents

Index document to primary index node (0.0.0.0:5000) with the following command:

```shell script
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -d "$(jq -c '. | @json' ./examples/bulk_put.jsonl | jq -s -c '{ docs:. }')" -plaintext 0.0.0.0:5000 phalanx.IndexService/BulkSet
```

Then commit index with the following command:

```shell script
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -plaintext 0.0.0.0:5000 phalanx.IndexService/Commit
```

If the commit is successful, the index will be saved to object storage. You can see the uploaded index at the following URL:
http://localhost:9000/minio/phalanx/index0/shard0/

The replica node detects that the index has been updated and downloads the latest index from object storage.
You can get document from index replica node (0.0.0.0:5001) with the following command:

```shell script
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -d '{ "id": "1" }' -plaintext 0.0.0.0:5001 phalanx.IndexService/Get | jq -r '.doc' | jq .
```
