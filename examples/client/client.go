package main

import (
	"fmt"
	etcd "github.com/coreos/etcd/client"
	grpclb "github.com/liyue201/grpc-lb"
	"github.com/liyue201/grpc-lb/examples/proto"
	cr "github.com/liyue201/grpc-lb/registry/consul"
	registry "github.com/liyue201/grpc-lb/registry/etcd"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"time"
)

func TestRandomLoadBalancer() {
	etcdConfg := etcd.Config{
		Endpoints: []string{"http://120.24.44.201:4001"},
	}
	r := registry.NewResolver("/grpc-lb", "test", etcdConfg)
	b := grpclb.NewBalancer(r, grpclb.NewRandomSelector())
	c, err := grpc.Dial("", grpc.WithInsecure(), grpc.WithBalancer(b), grpc.WithTimeout(time.Second))
	if err != nil {
		log.Printf("grpc dial: %s", err)
		return
	}
	client := proto.NewTestClient(c)

	for i := 0; i < 100; i++ {
		resp, err := client.Hello(context.Background(), &proto.HelloReq{Ping: "haha"})
		if err != nil {
			log.Println(err)
			continue
		}
		log.Printf(resp.Pong)
		time.Sleep(time.Second)
	}
}

func TestRoundRobinLoadBalancer() {
	etcdConfg := etcd.Config{
		Endpoints: []string{"http://120.24.44.201:4001"},
	}
	r := registry.NewResolver("/grpc-lb", "test", etcdConfg)
	b := grpclb.NewBalancer(r, grpclb.NewRoundRobinSelector())
	c, err := grpc.Dial("", grpc.WithInsecure(), grpc.WithBalancer(b), grpc.WithTimeout(time.Second))
	if err != nil {
		log.Printf("grpc dial: %s", err)
		return
	}
	client := proto.NewTestClient(c)

	for i := 0; i < 100; i++ {
		resp, err := client.Hello(context.Background(), &proto.HelloReq{Ping: "haha"})
		if err != nil {
			log.Println(err)
			continue
		}
		log.Printf(resp.Pong)
		time.Sleep(time.Second)
	}
}

func TestKetamaLoadBalancer() {
	etcdConfg := etcd.Config{
		Endpoints: []string{"http://120.24.44.201:4001"},
	}
	r := registry.NewResolver("/grpc-lb", "test", etcdConfg)
	b := grpclb.NewBalancer(r, grpclb.NewKetamaSelector(grpclb.DefaultKetamaKey))
	c, err := grpc.Dial("", grpc.WithInsecure(), grpc.WithBalancer(b))
	if err != nil {
		log.Printf("grpc dial: %s", err)
		return
	}
	client := proto.NewTestClient(c)

	for i := 0; i < 100; i++ {
		ctx := context.Background()
		resp, err := client.Hello(context.WithValue(ctx, grpclb.DefaultKetamaKey, fmt.Sprintf("aaaa %d", i)), &proto.HelloReq{Ping: "haha"})
		if err != nil {
			log.Println(err)
			continue
		}
		log.Printf(resp.Pong)
		time.Sleep(time.Second)
	}
}

func TestConsulRandomLoadBalancer() {
	r := cr.NewResolver("test", "http://120.24.44.201:8500")
	b := grpclb.NewBalancer(r, grpclb.NewRandomSelector())
	c, err := grpc.Dial("", grpc.WithInsecure(), grpc.WithBalancer(b), grpc.WithTimeout(time.Second*20))
	if err != nil {
		log.Printf("grpc dial: %s", err)
		return
	}
	client := proto.NewTestClient(c)

	for i := 0; i < 100; i++ {
		resp, err := client.Hello(context.Background(), &proto.HelloReq{Ping: "haha"})
		if err != nil {
			log.Println(err)
			continue
		}
		log.Printf(resp.Pong)
		time.Sleep(time.Second)
	}
}

func main() {
	TestRandomLoadBalancer()
	//TestRoundRobinLoadBalancer()
	//TestKetamaLoadBalancer()

	//TestConsulRandomLoadBalancer()
}
