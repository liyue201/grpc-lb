# grpc-lb
This is a gRPC load balancing library for go.

 ![](/architecture.png)
 
## Feature
- supports Random, RoundRobin, LeastConnection and ConsistentHash strategies.
- supports [etcd](https://github.com/etcd-io/etcd),[consul](https://github.com/consul/consul) and [zookeeper](https://github.com/apache/zookeeper) as a registry.

## Example

``` go
package main

import (
	etcd "github.com/coreos/etcd/client"
	"github.com/liyue201/grpc-lb/balancer"
	"github.com/liyue201/grpc-lb/examples/proto"
	registry "github.com/liyue201/grpc-lb/registry/etcd"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"time"
)

func main() {
	etcdConfg := etcd.Config{
		Endpoints: []string{"http://10.0.101.68:2379"},
	}
	registry.RegisterResolver("etcd", etcdConfg, "/backend/services", "test", "1.0")

	c, err := grpc.Dial("etcd:///", grpc.WithInsecure(), grpc.WithBalancerName(balancer.RoundRobin))
	if err != nil {
		log.Printf("grpc dial: %s", err)
		return
	}
	defer c.Close()

	client := proto.NewTestClient(c)
	for i := 0; i < 500; i++ {

		resp, err := client.Say(context.Background(), &proto.SayReq{Content: "round robin"})
		if err != nil {
			log.Println(err)
			time.Sleep(time.Second)
			continue
		}
		time.Sleep(time.Second)
		log.Printf(resp.Content)
	}
}
```
see more [examples](/examples)


## Stargazers over time

[![Stargazers over time](https://starchart.cc/liyue201/grpc-lb.svg)](https://starchart.cc/liyue201/grpc-lb)

