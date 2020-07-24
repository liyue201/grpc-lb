package consul

import (
	"context"
	"encoding/json"
	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/api/watch"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/resolver"
	"sync"
)

type ConsulWatcher struct {
	sync.RWMutex
	consulConf  *api.Config
	serviceName string
	wp          *watch.Plan
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	addrs       []resolver.Address
	addrsChan   chan []resolver.Address
}

func newConsulWatcher(serviceName string, conf *api.Config) *ConsulWatcher {
	wp, err := watch.Parse(map[string]interface{}{
		"type":    "service",
		"service": serviceName,
	})

	if err != nil {
		return nil
	}
	w := &ConsulWatcher{
		serviceName: serviceName,
		wp:          wp,
		consulConf:  conf,
		addrsChan:   make(chan []resolver.Address, 10),
	}
	wp.Handler = w.handle

	return w
}

func (w *ConsulWatcher) Close() {
	w.wp.Stop()
	w.wg.Wait()
	close(w.addrsChan)
}

func (w *ConsulWatcher) Watch() chan []resolver.Address {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		w.wp.RunWithConfig(w.consulConf.Address, w.consulConf)
	}()
	return w.addrsChan
}

func (w *ConsulWatcher) handle(idx uint64, data interface{}) {
	entries, ok := data.([]*api.ServiceEntry)
	if !ok {
		return
	}
	if w.wp.IsStopped() {
		return
	}
	addrs := []resolver.Address{}

	for _, e := range entries {
		for _, check := range e.Checks {
			if check.ServiceID == e.Service.ID {
				if check.Status == api.HealthPassing {
					md := metadata.MD{}
					if len(e.Service.Tags) > 0 {
						err := json.Unmarshal([]byte(e.Service.Tags[0]), &md)
						if err != nil {
							grpclog.Infof("Parse node data error:", err)
						}
					}
					addrs = append(addrs, resolver.Address{Addr: e.Service.Address, Metadata: &md})
				}
				break
			}
		}
	}
	if !isSameAddrs(w.addrs, addrs) {
		w.addrs = addrs
		w.addrsChan <- w.cloneAddresses(w.addrs)
	}
}

func (w *ConsulWatcher) cloneAddresses(in []resolver.Address) []resolver.Address {
	out := make([]resolver.Address, len(in))
	for i := 0; i < len(in); i++ {
		out[i] = in[i]
	}
	return out
}

func isSameAddrs(addrs1, addrs2 []resolver.Address) bool {
	if len(addrs1) != len(addrs2) {
		return false
	}
	for _, addr1 := range addrs1 {
		found := false
		for _, addr2 := range addrs2 {
			if addr1.Addr == addr2.Addr {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
