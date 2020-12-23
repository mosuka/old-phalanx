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
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -plaintext 0.0.0.0:5000 phalanx.IndexService/Schema | jq -r '.schema | @base64d' | jq .
```

#### Index document

You can index document with the following command:

```shell script
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -d "$(jq -c '. | {doc:@base64}' ./examples/doc_1.json)" -plaintext 0.0.0.0:5000 phalanx.IndexService/Set
```

Then commit index with the following command:

```shell script
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -plaintext 0.0.0.0:5000 phalanx.IndexService/Commit
```

#### Get a document

You can get document with the following command:

```shell script
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -d '{ "id": "1" }' -plaintext 0.0.0.0:5000 phalanx.IndexService/Get | jq -r '.doc | @base64d' | jq .
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
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -d "$(jq -c '. | @base64' ./examples/bulk_put.jsonl | jq -s -c '{ docs:. }')" -plaintext 0.0.0.0:5000 phalanx.IndexService/BulkSet
```

Then commit index with the following command:

```shell script
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -plaintext 0.0.0.0:5000 phalanx.IndexService/Commit
```

#### Search documents

You can search documents with the following command:

```shell script
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -d "$(jq '. | @base64' ./examples/search_request.json  | jq -s -c '{ request:. }')" -plaintext 0.0.0.0:5000 phalanx.IndexService/Search | jq -r '.result | @base64d' | jq .
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


### Start cluster with Docker compose

The sample cluster can be started with `docker-compose.yml`. This file will also launch components such as [etcd](https://etcd.io/), which is required for the service discovery backend, and [MinIO](https://min.io/), which is required for the storage backend.
You can start a cluster with the following command;

```shell script
$ docker-compose up
```

#### Make a request via dispatcher node

In cluster mode, make a request to the dispatcher node. The dispatcher node will dispatch the query and document to the appropriate index node.

Get document:
```shell script
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -d '{ "index_name": "index0", "id": "1" }' -plaintext 0.0.0.0:5200 phalanx.DispatcherService/Get | jq -r '.doc | @base64d' | jq .
```

Index document:
```shell script
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -d "$(jq -c '. | {"index_name": "index0", "route_field_name": "id", "doc": @base64}' ./examples/doc_1.json)" -plaintext 0.0.0.0:5200 phalanx.DispatcherService/Set
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -d '{ "index_name": "index0" }' -plaintext 0.0.0.0:5200 phalanx.DispatcherService/Commit
```

Delete document:
```shell script
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -d '{ "index_name": "index0", "id": "1" }' -plaintext 0.0.0.0:5200 phalanx.DispatcherService/Delete
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -d '{ "index_name": "index0" }' -plaintext 0.0.0.0:5200 phalanx.DispatcherService/Commit
```

Index documents in bulk:
```shell script
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -d "$(jq -c '. | @base64' ./examples/bulk_put.jsonl | jq -s -c '{ "index_name": "index0", "route_field_name": "id", "docs": . }')" -plaintext 0.0.0.0:5200 phalanx.DispatcherService/BulkSet
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -d '{ "index_name": "index0" }' -plaintext 0.0.0.0:5200 phalanx.DispatcherService/Commit
```

Delete documents in bulk:
```shell script
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -d "$(jq '. | @json' ./examples/bulk_delete.txt  | jq -s -c '{ "index_name": "index0", "ids": . }')" -plaintext 0.0.0.0:5200 phalanx.DispatcherService/BulkDelete
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -d '{ "index_name": "index0" }' -plaintext 0.0.0.0:5200 phalanx.DispatcherService/Commit
```

Rollback updates:
```shell script
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -d '{ "index_name": "index0" }' -plaintext 0.0.0.0:5200 phalanx.DispatcherService/Rollback
```

Merge index:
```shell script
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -d '{ "index_name": "index0" }' -plaintext 0.0.0.0:5200 phalanx.DispatcherService/Merge
```

Search documents:

```shell script
$ grpcurl -proto phalanx-proto/proto/phalanx.proto -d "$(jq '. | @base64' ./examples/search_request.json  | jq -s -c '{ "index_name": "index0", "request":. }')" -plaintext 0.0.0.0:5200 phalanx.DispatcherService/Search | jq -r '.result | @base64d' | jq .
```
