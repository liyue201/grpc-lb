# grpc-lb
This is a gRPC load balancing library for go.

 ![](/struct.png)
 
## Feature
- supports Random,RoundRobin and consistent-hash strategies.
- supports [etcd](https://github.com/etcd-io/etcd),[consul](https://github.com/consul/consul) and [zookeeper](https://github.com/apache/zookeeper) as registry.

## Example

``` go
package main

import (
	etcd "github.com/coreos/etcd/client"
	"github.com/liyue201/grpc-lb/examples/proto"
	registry "github.com/liyue201/grpc-lb/registry/etcd"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"time"
	"github.com/liyue201/grpc-lb/balancer"
)

func main() {
	etcdConfg := etcd.Config{
		Endpoints: []string{"http://144.202.111.210:2379"},
	}
	registry.RegisterResolver( "etcd", etcdConfg, "test", "v1.0")

	c, err := grpc.Dial("etcd:///",  grpc.WithInsecure(), grpc.WithBalancerName(balancer.RoundRobin))
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


-----------------------------------------------------------------
## 欢迎打赏

走过路过的大佬，如果这个项目对您帮助，请往这扔几个铜板

- BTC: 16L9w2vMn8XSFV7Ytar2LzHEsdDp3w9gM3  
- ETH: 0xE00a72aFb1890Bc4d0dcf2561aB26099cACEcD87  
- EOS: eosbetkiller  
