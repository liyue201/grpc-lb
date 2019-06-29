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
	registry.InitEtcdResolver(etcdConfg, "test", "v1.0")

	c, err := grpc.Dial(registry.EtcdTarget,  grpc.WithInsecure(), grpc.WithBalancerName(balancer.Random))
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
