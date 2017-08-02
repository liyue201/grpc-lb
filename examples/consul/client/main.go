package main

import (
	grpclb "github.com/liyue201/grpc-lb"
	"github.com/liyue201/grpc-lb/examples/proto"
	"github.com/liyue201/grpc-lb/registry/consul"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
)

func main() {
	r := consul.NewResolver("test", "http://120.24.44.201:8500")
	b := grpclb.NewBalancer(r, grpclb.NewRandomSelector())
	c, err := grpc.Dial("", grpc.WithInsecure(), grpc.WithBalancer(b))
	if err != nil {
		log.Printf("grpc dial: %s", err)
		return
	}
	defer c.Close()

	client := proto.NewTestClient(c)

	resp, err := client.Say(context.Background(), &proto.SayReq{Content: "consul"})
	if err != nil {
		log.Println(err)
		return
	}
	log.Printf(resp.Content)

}
