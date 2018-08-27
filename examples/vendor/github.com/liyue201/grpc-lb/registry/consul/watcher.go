package consul

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/consul/api"
	"github.com/hashicorp/consul/watch"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/naming"
	"sync"
)

// ConsulWatcher is the implementation of grpc.naming.Watcher
type ConsulWatcher struct {
	sync.RWMutex
	serviceName string
	wp          *watch.Plan
	updates     chan []*naming.Update
	addrs       []*naming.Update
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

	addrs := []*naming.Update{}

	for _, e := range entries {
		for _, check := range e.Checks {
			if check.ServiceID == e.Service.ID {
				if check.Status == api.HealthPassing {
					addr := fmt.Sprintf("%s:%d", e.Service.Address, e.Service.Port)
					metadata := map[string]string{}
					if len(e.Service.Tags) > 0 {
						err := json.Unmarshal([]byte(e.Service.Tags[0]), &metadata)
						if err != nil {
							grpclog.Println("Parse node data error:", err)
						}
					}
					addrs = append(addrs, &naming.Update{Addr: addr, Metadata: &metadata})
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
			if newAddr.Addr == oldAddr.Addr {
				found = true
				break
			}
		}
		if !found {
			newAddr.Op = naming.Add
			updates = append(updates, newAddr)
		}
	}

	//delete old address
	for _, oldAddr := range w.addrs {
		found := false
		for _, addr := range addrs {
			if addr.Addr == oldAddr.Addr {
				found = true
				break
			}
		}
		if !found {
			oldAddr.Op = naming.Delete
			updates = append(updates, oldAddr)
		}
	}
	w.addrs = addrs

	w.updates <- updates
}
