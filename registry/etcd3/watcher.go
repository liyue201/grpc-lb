package etcd

import (
	"encoding/json"
	etcd3 "go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"golang.org/x/net/context"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/resolver"
	"sync"
)

type Watcher struct {
	key    string
	client *etcd3.Client
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	addrs  []resolver.Address
}

func (w *Watcher) Close() {
	w.cancel()
}

func newWatcher(key string, cli *etcd3.Client) *Watcher {
	ctx, cancel := context.WithCancel(context.Background())
	w := &Watcher{
		key:    key,
		client: cli,
		ctx:    ctx,
		cancel: cancel,
	}
	return w
}

func (w *Watcher) GetAllAddresses() []resolver.Address {
	ret := []resolver.Address{}

	resp, err := w.client.Get(w.ctx, w.key, etcd3.WithPrefix())
	if err == nil {
		addrs := extractAddrs(resp)
		if len(addrs) > 0 {
			for _, addr := range addrs {
				v := addr
				ret = append(ret, resolver.Address{
					Addr:     v.Addr,
					Metadata: &v.Metadata,
				})
			}
		}
	}
	return ret
}

func (w *Watcher) Watch() chan []resolver.Address {
	out := make(chan []resolver.Address, 10)
	w.wg.Add(1)
	go func() {
		defer func() {
			close(out)
			w.wg.Done()
		}()
		addrs := w.GetAllAddresses()
		out <- w.cloneAddresses(addrs)

		rch := w.client.Watch(w.ctx, w.key, etcd3.WithPrefix())
		for wresp := range rch {
			for _, ev := range wresp.Events {
				switch ev.Type {
				case mvccpb.PUT:
					nodeData := NodeData{}
					err := json.Unmarshal([]byte(ev.Kv.Value), &nodeData)
					if err != nil {
						grpclog.Error("Parse node data error:", err)
						continue
					}
					addr := resolver.Address{Addr: nodeData.Addr, Metadata: &nodeData.Metadata}
					if w.addAddr(addr) {
						out <- w.cloneAddresses(w.addrs)
					}
				case mvccpb.DELETE:
					nodeData := NodeData{}
					err := json.Unmarshal([]byte(ev.Kv.Value), &nodeData)
					if err != nil {
						grpclog.Error("Parse node data error:", err)
						continue
					}
					addr := resolver.Address{Addr: nodeData.Addr, Metadata: &nodeData.Metadata}
					if w.removeAddr(addr) {
						out <- w.cloneAddresses(w.addrs)
					}
				}
			}
		}
	}()
	return out
}

func extractAddrs(resp *etcd3.GetResponse) []NodeData {
	addrs := []NodeData{}

	if resp == nil || resp.Kvs == nil {
		return addrs
	}

	for i := range resp.Kvs {
		if v := resp.Kvs[i].Value; v != nil {
			nodeData := NodeData{}
			err := json.Unmarshal(v, &nodeData)
			if err != nil {
				grpclog.Info("Parse node data error:", err)
				continue
			}
			addrs = append(addrs, nodeData)
		}
	}
	return addrs
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
