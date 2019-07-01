package zk

import (
	"encoding/json"
	"github.com/samuel/go-zookeeper/zk"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/resolver"
	"strings"
	"sync"
	"time"
)

type Watcher struct {
	zkServers []string
	conn      *zk.Conn
	path      string
	addrs     []resolver.Address
	addrsChan chan []resolver.Address
	wg        sync.WaitGroup
}

func newWatcher(zkServers []string, path string) (*Watcher, error) {
	w := &Watcher{
		zkServers: zkServers,
		addrsChan: make(chan []resolver.Address, 10),
		path:      path,
	}
	c, _, err := zk.Connect(zkServers, time.Second*15)
	if err != nil {
		return nil, err
	}
	w.conn = c
	return w, nil
}

func (w *Watcher) Watch() chan []resolver.Address {
	if exist, _, _ := w.conn.Exists(w.path); !exist {
		err := w.createPath(w.path)
		if err != nil {
			grpclog.Infof("Watcher create path error, %v", err)
			return w.addrsChan
		}
	}
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		for {
			children, _, eventCh, err := w.conn.ChildrenW(w.path)
			if err != nil {
				grpclog.Errorf("Watcher ChildrenW: %v", err.Error())
				continue
			}

			addrs := []resolver.Address{}
			for _, child := range children {
				data, _, err := w.conn.Get(w.path + "/" + child)
				if err != nil {
					continue
				}
				nodeData := NodeData{}
				err = json.Unmarshal(data, &nodeData)
				if err != nil {
					continue
				}
				addrs = append(addrs, resolver.Address{Addr: nodeData.Addr, Metadata: &nodeData.Metadata})
			}

			if !isSameAddrs(w.addrs, addrs) {
				w.addrs = addrs
				w.addrsChan <- w.cloneAddresses(addrs)
			}
			for range eventCh {
				//do nothing
			}
		}
	}()
	return w.addrsChan
}

func (w *Watcher) createPath(path string) error {
	znodes := strings.Split(path, "/")
	var onepath string
	for _, znode := range znodes {
		if len(znode) == 0 {
			continue
		}
		onepath = onepath + "/" + znode
		exists, _, _ := w.conn.Exists(onepath)
		if exists {
			continue
		}
		err := createtNode(w.conn, onepath)
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *Watcher) Close() {
	w.conn.Close()
	w.wg.Wait()
}

func (w *Watcher) cloneAddresses(in []resolver.Address) []resolver.Address {
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
