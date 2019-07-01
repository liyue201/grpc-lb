package main

import (
	"fmt"
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
		Endpoints: []string{"http://144.202.111.210:2379"},
	}
	balancer.InitConsistentHashBuilder(balancer.DefaultConsistentHashKey)
	registry.RegisterResolver("etcd", etcdConfg, "test", "v1.0")

	c, err := grpc.Dial("etcd:///",  grpc.WithInsecure(), grpc.WithBalancerName(balancer.ConsistentHash))
	if err != nil {
		log.Printf("grpc dial: %s", err)
		return
	}
	defer c.Close()

	client := proto.NewTestClient(c)
	for i := 0; i < 100; i++ {
		ctx := context.Background()

		hashData := fmt.Sprintf("aaaa %d", i)
		resp, err := client.Say(context.WithValue(ctx, balancer.DefaultConsistentHashKey, hashData),
			&proto.SayReq{Content: "ketama"})
		if err != nil {
			log.Println(err)
			time.Sleep(time.Second)
			continue
		}
		log.Printf(resp.Content)
		time.Sleep(time.Second)
	}
}
