package consul

import (
	"fmt"
	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/watch"
	"golang.org/x/net/context"
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
	ctx         context.Context
	cancel      context.CancelFunc
}

func newConsulWatcher(serviceName string, address string) naming.Watcher {
	ctx, cancel := context.WithCancel(context.Background())

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
		ctx:         ctx,
		cancel:      cancel,
		updates:     make(chan []*naming.Update),
	}
	wp.Handler = w.handle
	go wp.Run(address)
	return w
}

func (w *ConsulWatcher) Close() {
	w.wp.Stop()
	w.cancel()
	close(w.updates)
	<-w.ctx.Done()
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
			if check.CheckID == e.Service.ID {
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
		for _, addr := range w.addrs {
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
