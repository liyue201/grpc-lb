package main

import (
	"fmt"
	grpclb "github.com/liyue201/grpc-lb"
	"github.com/liyue201/grpc-lb/examples/proto"
	registry "github.com/liyue201/grpc-lb/registry/etcd"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"time"
)

func TestRandomLoadBalancer() {
	r := registry.NewResolver("/grpc-lb", "test")
	b := grpclb.NewBalancer(r, grpclb.NewRandomSelector())
	c, err := grpc.Dial("http://120.24.44.201:4001", grpc.WithInsecure(), grpc.WithBalancer(b), grpc.WithTimeout(time.Second))
	if err != nil {
		log.Printf("grpc dial: %s", err)
		return
	}
	client := proto.NewTestClient(c)

	for i := 0; i < 10; i++ {
		resp, err := client.Hello(context.Background(), &proto.HelloReq{Ping: "haha"})
		if err != nil {
			log.Println(err)
			return
		}
		log.Printf(resp.Pong)
	}
}

func TestRoundRobinLoadBalancer() {
	r := registry.NewResolver("/grpc-lb", "test")
	b := grpclb.NewBalancer(r, grpclb.NewRoundRobinSelector())
	c, err := grpc.Dial("http://120.24.44.201:4001", grpc.WithInsecure(), grpc.WithBalancer(b), grpc.WithTimeout(time.Second))
	if err != nil {
		log.Printf("grpc dial: %s", err)
		return
	}
	client := proto.NewTestClient(c)

	for i := 0; i < 10; i++ {
		resp, err := client.Hello(context.Background(), &proto.HelloReq{Ping: "haha"})
		if err != nil {
			log.Println(err)
			return
		}
		log.Printf(resp.Pong)
	}
}

func TestKetamaLoadBalancer() {
	r := registry.NewResolver("/grpc-lb", "test")
	b := grpclb.NewBalancer(r, grpclb.NewKetamaSelector(grpclb.DefaultKetamaKey))
	c, err := grpc.Dial("http://120.24.44.201:4001", grpc.WithInsecure(), grpc.WithBalancer(b), grpc.WithTimeout(time.Second))
	if err != nil {
		log.Printf("grpc dial: %s", err)
		return
	}
	client := proto.NewTestClient(c)

	for i := 0; i < 10; i++ {
		ctx := context.Background()
		resp, err := client.Hello(context.WithValue(ctx, grpclb.DefaultKetamaKey, fmt.Sprintf("aaaa %d", i)), &proto.HelloReq{Ping: "haha"})
		if err != nil {
			log.Println(err)
			return
		}
		log.Printf(resp.Pong)
	}
}

func main() {
	//TestRandomLoadBalancer()
	//TestRoundRobinLoadBalancer()
	//TestKetamaLoadBalancer()
}
