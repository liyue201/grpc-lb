package consul

import (
	"errors"
	"google.golang.org/grpc/naming"
)

// ConsulResolver is the implementaion of grpc.naming.Resolver
type ConsulResolver struct {
	serviceName string
	consulAddr  string
}

// NewResolver return ConsulResolver with service name
func NewResolver(serviceName string, consulAddr string) *ConsulResolver {
	return &ConsulResolver{serviceName: serviceName, consulAddr: consulAddr}
}

// Resolve to resolve the service from consul
func (c *ConsulResolver) Resolve(target string) (naming.Watcher, error) {
	if c.serviceName == "" {
		return nil, errors.New("no service name provided")
	}
	// return ConsulWatcher
	watcher := newConsulWatcher(c.serviceName, c.consulAddr)
	return watcher, nil
}
