package etcd

import (
	"errors"
	"fmt"
	etcd "github.com/coreos/etcd/client"
	"google.golang.org/grpc/naming"
)

// EtcdResolver is an implementation of grpc.naming.Resolver
type EtcdResolver struct {
	Config      etcd.Config
	RegistryDir string
	ServiceName string
}

func NewResolver(registryDir, serviceName string, cfg etcd.Config) naming.Resolver {
	return &EtcdResolver{RegistryDir: registryDir, ServiceName: serviceName, Config: cfg}
}

// Resolve to resolve the service from etcd
func (er *EtcdResolver) Resolve(target string) (naming.Watcher, error) {
	if er.ServiceName == "" {
		return nil, errors.New("no service name provided")
	}
	client, err := etcd.New(er.Config)
	if err != nil {
		return nil, err
	}

	key := fmt.Sprintf("%s/%s", er.RegistryDir, er.ServiceName)
	return newEtcdWatcher(key, client), nil
}
