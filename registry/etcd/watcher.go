package etcd

import (
	"encoding/json"
	"fmt"
	etcd_cli "github.com/coreos/etcd/client"
	"golang.org/x/net/context"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/resolver"
	"sync"
)

type Watcher struct {
	key     string
	keyapi  etcd_cli.KeysAPI
	watcher etcd_cli.Watcher
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	addrs   []resolver.Address
}

func (w *Watcher) Close() {
	w.cancel()
	w.wg.Wait()
}

func newWatcher(key string, cli etcd_cli.Client) *Watcher {

	api := etcd_cli.NewKeysAPI(cli)
	watcher := api.Watcher(key, &etcd_cli.WatcherOptions{Recursive: true})
	ctx, cancel := context.WithCancel(context.Background())

	w := &Watcher{
		key:     key,
		keyapi:  api,
		watcher: watcher,
		ctx:     ctx,
		cancel:  cancel,
	}
	return w
}

func (w *Watcher) GetAllAddresses() []resolver.Address {
	resp, _ := w.keyapi.Get(w.ctx, w.key, &etcd_cli.GetOptions{Recursive: true})
	addrs := []resolver.Address{}
	for _, n := range resp.Node.Nodes {
		nodeData := NodeData{}

		err := json.Unmarshal([]byte(n.Value), &nodeData)
		if err != nil {
			grpclog.Infof("Parse node data error:", err)
			continue
		}
		addrs = append(addrs, resolver.Address{
			Addr:     nodeData.Addr,
			Metadata: &nodeData.Metadata,
		})
	}
	fmt.Printf("[GetAllAddresses] %v\n", addrs)
	return addrs
}

func (w *Watcher) Watch() chan []resolver.Address {
	out := make(chan []resolver.Address, 10)
	w.wg.Add(1)
	go func() {
		defer func() {
			close(out)
			w.wg.Done()
		}()

		w.addrs = w.GetAllAddresses()
		out <- w.cloneAddresses(w.addrs)

		for {
			resp, err := w.watcher.Next(w.ctx)
			if err != nil {
				grpclog.Errorf("etcd Watcher: %s", err.Error())
				if err == context.Canceled {
					return
				}
				continue
			}
			if resp.Node.Dir {
				continue
			}
			nodeData := NodeData{}

			if resp.Action == "set" || resp.Action == "create" || resp.Action == "update" ||
				resp.Action == "delete" || resp.Action == "expire" {
				err := json.Unmarshal([]byte(resp.Node.Value), &nodeData)
				if err != nil {
					grpclog.Infof("Parse node data error:", err)
					continue
				}
				addr := resolver.Address{Addr: nodeData.Addr, Metadata: &nodeData.Metadata}
				changed := false
				switch resp.Action {
				case "set", "create":
					changed = w.addAddr(addr)
				case "update":
					changed = w.updateAddr(addr)
				case "delete", "expire":
					changed = w.removeAddr(addr)
				}
				if changed {
					out <- w.cloneAddresses(w.addrs)
				}
			}
		}
	}()
	return out
}

func (w *Watcher) cloneAddresses(in []resolver.Address) []resolver.Address {
	out := make([]resolver.Address, len(in))
	for i := 0; i < len(in); i++ {
		out[i] = in[i]
	}
	return out
}

func (w *Watcher) addAddr(addr resolver.Address) bool {
	for _, v := range w.addrs {
		if addr.Addr == v.Addr {
			return false
		}
	}
	w.addrs = append(w.addrs, addr)
	return true
}

func (w *Watcher) removeAddr(addr resolver.Address) bool {
	for i, v := range w.addrs {
		if addr.Addr == v.Addr {
			w.addrs = append(w.addrs[:i], w.addrs[i+1:]...)
			return true
		}
	}
	return false
}

func (w *Watcher) updateAddr(addr resolver.Address) bool {
	for i, v := range w.addrs {
		if addr.Addr == v.Addr {
			w.addrs[i] = addr
			return true
		}
	}
	return false
}
