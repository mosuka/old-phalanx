# Phalanx

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Phalanx is a cloud-based index writer and searcher written in [Rust](https://www.rust-lang.org/).  
Phalanx makes easy for programmers to develop search applications with advanced features and high availability.

## Docker compose

Start.

```shell script
$ sudo docker-compose up -d
$ sudo docker exec -it etcd1 etcdctl endpoint health
$ sudo docker exec -it etcd2 etcdctl endpoint health
$ sudo docker exec -it etcd3 etcdctl endpoint health
```

MinIO URL : `http://127.0.0.1:9001/`

Stop.

```shell script
$ sudo docker-compose down
```
