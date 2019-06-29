package main

import (

	etcd3 "go.etcd.io/etcd/clientv3"
	"github.com/liyue201/grpc-lb/balancer"
	registry "github.com/liyue201/grpc-lb/registry/etcd3"
	"github.com/liyue201/grpc-lb/examples/proto"
	"golang.org/x/net/context"
	"log"
	"time"
	"google.golang.org/grpc"
)

func main() {
	etcdConfg := etcd3.Config{
		Endpoints: []string{"http://144.202.111.210:2379"},
	}
	registry.InitEtcdResolver(etcdConfg, "test", "v1.0")

	c, err := grpc.Dial(registry.EtcdTarget,  grpc.WithInsecure(), grpc.WithBalancerName(balancer.RoundRobin))
	if err != nil {
		log.Printf("grpc dial: %s", err)
		return
	}
	defer c.Close()
	client := proto.NewTestClient(c)

	for i := 0; i < 500; i++ {
		resp, err := client.Say(context.Background(), &proto.SayReq{Content: "round robin"})
		if err != nil {
			log.Println("aa:", err)
			time.Sleep(time.Second)
			continue
		}
		time.Sleep(time.Second)
		log.Printf(resp.Content)
	}
}
