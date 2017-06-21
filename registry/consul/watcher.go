package consul

import (
	"fmt"
	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/watch"
	"google.golang.org/grpc/naming"
	"sync"
)

// ConsulWatcher is the implementation of grpc.naming.Watcher
type ConsulWatcher struct {
	sync.RWMutex
	serviceName string
	wp          *watch.WatchPlan
	updates     chan []*naming.Update
	addrs       []string
}

func newConsulWatcher(serviceName string, address string) naming.Watcher {
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
		updates:     make(chan []*naming.Update),
	}
	wp.Handler = w.handle
	go wp.Run(address)
	return w
}

func (w *ConsulWatcher) Close() {
	w.wp.Stop()
	close(w.updates)
}

func (w *ConsulWatcher) Next() ([]*naming.Update, error) {
	select {
	case updates := <-w.updates:
		return updates, nil
	}
	return []*naming.Update{}, nil
}

func (w *ConsulWatcher) handle(idx uint64, data interface{}) {
	entries, ok := data.([]*api.ServiceEntry)
	if !ok {
		return
	}

	addrs := []string{}

	for _, e := range entries {

		for _, check := range e.Checks {
			if check.ServiceID == e.Service.ID {
				if check.Status == api.HealthPassing {
					addr := fmt.Sprintf("%s:%d", e.Service.Address, e.Service.Port)
					addrs = append(addrs, addr)
				}
				break
			}
		}
	}

	updates := []*naming.Update{}

	//add new address
	for _, newAddr := range addrs {
		found := false
		for _, oldAddr := range w.addrs {
			if newAddr == oldAddr {
				found = true
				break
			}
		}
		if !found {
			updates = append(updates, &naming.Update{Addr: newAddr, Op: naming.Add})
		}
	}

	//delete old address
	for _, oldAddr := range w.addrs {
		found := false
		for _, addr := range addrs {
			if addr == oldAddr {
				found = true
				break
			}
		}
		if !found {
			updates = append(updates, &naming.Update{Addr: oldAddr, Op: naming.Delete})
		}
	}
	w.addrs = addrs

	w.updates <- updates
}
