# phalanx


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
