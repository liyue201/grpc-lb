package main

import (
	"flag"
	"fmt"
	etcd "github.com/coreos/etcd/client"
	capi "github.com/hashicorp/consul/api"
	"github.com/liyue201/grpc-lb/examples/proto"
	cr "github.com/liyue201/grpc-lb/registry/consul"
	registry "github.com/liyue201/grpc-lb/registry/etcd"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"net"
	"sync"
	"time"
)

var nodeName = flag.String("node", "node1", "node name")
var port = flag.Int("port", 8080, "listening port")

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
	pong := "Hello " + req.Ping + ", I am " + *nodeName
	log.Println(pong)

	return &proto.HelloResp{Pong: pong}, nil
}

func testEtcd() {
	etcdConfg := etcd.Config{
		Endpoints: []string{"http://120.24.44.201:4001"},
	}
	registry, err := registry.NewRegistry(
		registry.Option{
			EtcdConfig:  etcdConfg,
			RegistryDir: "/grpc-lb",
			ServiceName: "test",
			NodeName:    *nodeName,
			NodeAddr:    fmt.Sprintf("127.0.0.1:%d", *port),
			Ttl:         10 * time.Second,
		})
	if err != nil {
		log.Panic(err)
		return
	}
	server := NewRpcServer(fmt.Sprintf("0.0.0.0:%d", *port))
	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		server.Run()
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		registry.Register()
		wg.Done()
	}()

	//stop the server after one minute
	go func(){
		time.Sleep(time.Minute)
		server.Stop()
		registry.Deregister()
	}()

	wg.Wait()
}

func testConsul() {
	config := &capi.Config{
		Address: "http://120.24.44.201:8500",
	}

	registry, err := cr.NewRegistry(
		&cr.Congfig{
			ConsulCfg:   config,
			ServiceName: "test",
			NodeID:      *nodeName,
			NodeAddress: "127.0.0.1",
			NodePort:    *port,
			Ttl:         5,
		})
	if err != nil {
		log.Panic(err)
		return
	}
	server := NewRpcServer(fmt.Sprintf("0.0.0.0:%d", *port))
	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		server.Run()
		wg.Done()
	}()
	wg.Add(1)
	go func() {
		registry.Register()
		wg.Done()
	}()

	//stop the server after one minute
	go func(){
		time.Sleep(time.Minute)
		server.Stop()
		registry.Deregister()
	}()

	wg.Wait()
}

//go run server.go -node node1 -port 28544
//go run server.go -node node2 -port 18562
//go run server.go -node node3 -port 27772
func main() {
	flag.Parse()

	testEtcd()
	//testConsul()
}
