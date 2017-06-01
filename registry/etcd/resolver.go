package etcd

import (
	"errors"
	"fmt"
	etcd "github.com/coreos/etcd/client"
	"google.golang.org/grpc/naming"
	"strings"
)

// EtcdResolver is an implementation of grpc.naming.Resolver
type EtcdResolver struct {
	RegistryDir string
	ServiceName string
}

func NewResolver(registryDir, serviceName string) *EtcdResolver {
	return &EtcdResolver{RegistryDir: registryDir, ServiceName: serviceName}
}

// Resolve to resolve the service from etcd, target is the dial address of etcd
// target example: "http://127.0.0.1:2379,http://127.0.0.1:12379,http://127.0.0.1:22379"
func (er *EtcdResolver) Resolve(target string) (naming.Watcher, error) {
	if er.ServiceName == "" {
		return nil, errors.New("no service name provided")
	}

	endpoints := strings.Split(target, ",")
	conf := etcd.Config{
		Endpoints: endpoints,
	}
	client, err := etcd.New(conf)
	if err != nil {
		return nil, err
	}

	key := fmt.Sprintf("%s/%s", er.RegistryDir, er.ServiceName)
	return newEtcdWatcher(key, client), nil
}
