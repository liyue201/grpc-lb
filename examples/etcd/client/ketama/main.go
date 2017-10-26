package main

import (
	"fmt"
	etcd "github.com/coreos/etcd/client"
	grpclb "github.com/liyue201/grpc-lb"
	"github.com/liyue201/grpc-lb/examples/proto"
	registry "github.com/liyue201/grpc-lb/registry/etcd"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"time"
)

func main() {
	etcdConfg := etcd.Config{
		Endpoints: []string{"http://120.24.44.201:2379"},
	}
	r := registry.NewResolver("/grpc-lb", "test", etcdConfg)
	b := grpclb.NewBalancer(r, grpclb.NewKetamaSelector(grpclb.DefaultKetamaKey))
	c, err := grpc.Dial("", grpc.WithInsecure(), grpc.WithBalancer(b), grpc.WithTimeout(time.Second))
	if err != nil {
		log.Printf("grpc dial: %s", err)
		return
	}
	client := proto.NewTestClient(c)

	for i := 0; i < 10; i++ {
		ctx := context.Background()

		hashData := fmt.Sprintf("aaaa %d", i)
		resp, err := client.Say(context.WithValue(ctx, grpclb.DefaultKetamaKey, hashData),
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
