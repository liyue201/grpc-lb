install consul
```
docker run -d --name consul -p 8500:8500 -e 'CONSUL_LOCAL_CONFIG={"leave_on_terminate": true}' consul agent -server  -bootstrap-expect 1  -bind=0.0.0.0 -client=0.0.0.0
```

install zookeeper
```
docker run --name zookeeper -p 2189:2181 -d zookeeper:3.4
```

install etcd
```
NODE1=10.0.101.68
docker run -d   -p 2379:2379  -p 2380:2380 \
--name etcd  \
quay.io/coreos/etcd:latest \
/usr/local/bin/etcd   --data-dir=/etcd-data --name node1 \
--initial-advertise-peer-urls http://${NODE1}:2380 \
--listen-peer-urls http://0.0.0.0:2380  \
--advertise-client-urls http://${NODE1}:2379 \
--listen-client-urls http://0.0.0.0:2379  \
--initial-cluster node1=http://${NODE1}:2380 
```
