package consul

import (
	"context"
	"fmt"
	consul "github.com/hashicorp/consul/api"
	"google.golang.org/grpc/grpclog"
	"time"
)

type ConsulRegistry struct {
	ctx     context.Context
	cancel  context.CancelFunc
	client  *consul.Client
	cfg     *Congfig
	checkId string
}

type Congfig struct {
	ConsulCfg   *consul.Config
	ServiceName string
	NodeID      string
	NodeAddress string
	NodePort    int
	Ttl         int //ttl seconds
}

func NewRegistry(cfg *Congfig) (*ConsulRegistry, error) {
	c, err := consul.NewClient(cfg.ConsulCfg)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())

	return &ConsulRegistry{
		ctx:    ctx,
		cancel: cancel,
		cfg:    cfg,
		client: c,
	}, nil
}

func (c *ConsulRegistry) Register() error {
	ticker := time.NewTicker(time.Duration(c.cfg.Ttl) * time.Second / 5)

	// initial register service
	regis := &consul.AgentServiceRegistration{
		ID:      c.cfg.NodeID,
		Name:    c.cfg.ServiceName,
		Address: c.cfg.NodeAddress,
		Port:    c.cfg.NodePort,
	}
	err := c.client.Agent().ServiceRegister(regis)
	if err != nil {
		return fmt.Errorf("initial register service to consul error: %s\n", err.Error())
	}

	// initial register service check
	c.checkId = c.cfg.ServiceName + ":" + c.cfg.NodeID
	check := consul.AgentServiceCheck{TTL: fmt.Sprintf("%ds", c.cfg.Ttl), Status: "passing"}
	err = c.client.Agent().CheckRegister(&consul.AgentCheckRegistration{
		ID:                c.checkId,
		Name:              c.cfg.ServiceName,
		AgentServiceCheck: check,
	})
	if err != nil {
		return fmt.Errorf("nitial register service check to consul error: %s", err.Error())
	}

	for {
		select {
		case <-c.ctx.Done():
			ticker.Stop()
			c.client.Agent().CheckDeregister(c.checkId)
			return nil
		case <-ticker.C:
			err := c.client.Agent().PassTTL(c.checkId, "")
			if err != nil {
				grpclog.Printf("consul registry check %v.\n", err)
			}
		}
	}

	return nil
}

func (c *ConsulRegistry) Deregister() error {
	c.cancel()
	return nil
}
