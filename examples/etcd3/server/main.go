package main

import (
	"flag"
	"fmt"
	"github.com/liyue201/grpc-lb/examples/proto"
	registry "github.com/liyue201/grpc-lb/registry/etcd3"
	etcd "github.com/coreos/etcd/clientv3"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var nodeID = flag.String("node", "node1", "node ID")
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

func (s *RpcServer) Say(ctx context.Context, req *proto.SayReq) (*proto.SayResp, error) {
	text := "Hello " + req.Content + ", I am " + *nodeID
	log.Println(text)

	return &proto.SayResp{Content: text}, nil
}

func StartService() {
	etcdConfg := etcd.Config{
		Endpoints: []string{"http://144.202.111.210:2379"},
	}

	registrar, err := registry.NewRegistrar(
		registry.Option{
			EtcdConfig:     etcdConfg,
			RegistryDir:    registry.RegistryDir,
			ServiceName:    "test",
			ServiceVersion: "v1.0",
			NodeID:         *nodeID,
			NData: registry.NodeData{
				Addr: fmt.Sprintf("127.0.0.1:%d", *port),
				//Metadata: map[string]string{"weight": "1"},
			},
			Ttl: 10 * time.Second,
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
		registrar.Register()
		wg.Done()
	}()

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	<-signalChan
	registrar.Unregister()
	server.Stop()
	wg.Wait()
}

//go run main.go -node node1 -port 28544
//go run main.go -node node2 -port 18562
//go run main.go -node node3 -port 27772
func main() {
	flag.Parse()
	StartService()
}
