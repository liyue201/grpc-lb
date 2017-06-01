package main

import (
	etcd "github.com/coreos/etcd/client"
	"github.com/liyue201/grpc-lb/examples/proto"
	registry "github.com/liyue201/grpc-lb/registry/etcd"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"net"
	"sync"
	"time"
)

type RpcServer struct {
	addr string
	s    *grpc.Server
}

func NewRpcServer(addr string) *RpcServer {
	s := grpc.NewServer()
	rs := &RpcServer{
		addr: addr,
		s:    s,
	}
	return rs
}

func (s *RpcServer) Run() {
	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		log.Printf("failed to listen: %v", err)
		return
	}
	log.Printf("rpc listening on:%s", s.addr)

	proto.RegisterTestServer(s.s, s)
	s.s.Serve(listener)
}

func (s *RpcServer) Stop() {
	s.s.GracefulStop()
}

func (s *RpcServer) Hello(ctx context.Context, req *proto.HelloReq) (*proto.HelloResp, error) {
	pong := "Hello " + req.Ping
	log.Println(pong)

	return &proto.HelloResp{Pong: pong}, nil
}

func main() {
	etcdConfg := etcd.Config{
		Endpoints: []string{"http://120.24.44.201:4001"},
	}
	registry, err := registry.NewRegistry(
		registry.Option{
			EtcdConfig:  etcdConfg,
			RegistryDir: "/grpc-lb",
			ServiceName: "test",
			NodeName:    "node1",
			NodeAddr:    "127.0.0.1:6262",
			Ttl:         10 * time.Second,
		})
	if err != nil {
		log.Panic(err)
		return
	}
	server := NewRpcServer("0.0.0.0:6262")
	wg := sync.WaitGroup{}

	wg.Add(2)
	go func() {
		server.Run()
		wg.Done()
	}()
	go func() {
		registry.Register()
		wg.Done()
	}()
	wg.Wait()
}
