# Phalanx

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

Phalanx is a cloud-based index writer and searcher written in [Rust](https://www.rust-lang.org/).  
Phalanx makes easy for programmers to develop search applications with advanced features and high availability.

```shell script
$ phalanx-discovery --host=0.0.0.0 \
                   --http-port=8100 \
                   --discovery-type=etcd \
                   --etcd-endpoints=127.0.0.1:2379 \
                   --etcd-root=/phalanx \
                   --watch-interval=500
```
