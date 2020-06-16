package consul

import (
	"context"
	"encoding/json"
	"fmt"
	consul "github.com/hashicorp/consul/api"
	"github.com/liyue201/grpc-lb/registry"
	"google.golang.org/grpc/grpclog"
	"sync"
	"time"
)

type Registrar struct {
	sync.RWMutex
	client   *consul.Client
	cfg      *Config
	canceler map[string]context.CancelFunc
}

type Config struct {
	ConsulCfg *consul.Config
	Ttl       int //ttl seconds
}

func NewRegistrar(cfg *Config) (*Registrar, error) {
	c, err := consul.NewClient(cfg.ConsulCfg)
	if err != nil {
		return nil, err
	}
	return &Registrar{
		canceler: make(map[string]context.CancelFunc),
		client:   c,
		cfg:      cfg,
	}, nil
}

func (c *Registrar) Register(service *registry.ServiceInfo) error {
	// register service
	metadata, err := json.Marshal(service.Metadata)
	if err != nil {
		return err
	}
	tags := make([]string, 0)
	tags = append(tags, string(metadata))

	register := func() error {
		regis := &consul.AgentServiceRegistration{
			ID:      service.InstanceId,
			Name:    service.Name + ":" + service.Version,
			Address: service.Address,
			Tags:    tags,
			Check: &consul.AgentServiceCheck{
				TTL:                            fmt.Sprintf("%ds", c.cfg.Ttl),
				Status:                         consul.HealthPassing,
				DeregisterCriticalServiceAfter: "1m",
			}}
		err := c.client.Agent().ServiceRegister(regis)
		if err != nil {
			return fmt.Errorf("register service to consul error: %s\n", err.Error())
		}
		return nil
	}

	err = register()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())

	c.Lock()
	c.canceler[service.InstanceId] = cancel
	c.Unlock()

	keepAliveTicker := time.NewTicker(time.Duration(c.cfg.Ttl) * time.Second / 5)
	registerTicker := time.NewTicker(time.Minute)

	for {
		select {
		case <-ctx.Done():
			keepAliveTicker.Stop()
			registerTicker.Stop()
			c.client.Agent().ServiceDeregister(service.InstanceId)
			return nil
		case <-keepAliveTicker.C:
			err := c.client.Agent().PassTTL("service:"+service.InstanceId, "")
			if err != nil {
				grpclog.Infof("consul registry check %v.\n", err)
			}
		case <-registerTicker.C:
			err = register()
			if err != nil {
				grpclog.Infof("consul register service error: %v.\n", err)
			}
		}
	}

	return nil
}

func (c *Registrar) Unregister(service *registry.ServiceInfo) error {
	c.RLock()
	cancel, ok := c.canceler[service.InstanceId]
	c.RUnlock()

	if ok {
		cancel()
	}
	return nil
}


func (r *Registrar) Close() {

}
