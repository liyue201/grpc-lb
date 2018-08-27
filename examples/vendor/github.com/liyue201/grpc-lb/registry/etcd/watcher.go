package etcd

import (
	"encoding/json"
	etcd "github.com/coreos/etcd/client"
	"golang.org/x/net/context"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/naming"
)

// EtcdWatcher is the implementation of grpc.naming.Watcher
type EtcdWatcher struct {
	key     string
	keyapi  etcd.KeysAPI
	watcher etcd.Watcher
	updates []*naming.Update
	ctx     context.Context
	cancel  context.CancelFunc
}

func (w *EtcdWatcher) Close() {
	w.cancel()
}

func newEtcdWatcher(key string, cli etcd.Client) naming.Watcher {

	api := etcd.NewKeysAPI(cli)
	watcher := api.Watcher(key, &etcd.WatcherOptions{Recursive: true})
	ctx, cancel := context.WithCancel(context.Background())

	w := &EtcdWatcher{
		key:     key,
		keyapi:  api,
		watcher: watcher,
		ctx:     ctx,
		cancel:  cancel,
	}
	return w
}

func (w *EtcdWatcher) Next() ([]*naming.Update, error) {

	updates := []*naming.Update{}
	if len(w.updates) == 0 {
		resp, _ := w.keyapi.Get(w.ctx, w.key, &etcd.GetOptions{Recursive: true})

		for _, n := range resp.Node.Nodes {
			nodeData := NodeData{}

			err := json.Unmarshal([]byte(n.Value), &nodeData)
			if err != nil {
				grpclog.Println("Parse node data error:", err)
				continue
			}
			updates = append(updates, &naming.Update{
				Op:       naming.Add,
				Addr:     nodeData.Addr,
				Metadata: &nodeData.Metadata,
			})
		}

		if len(updates) != 0 {
			w.updates = updates
			return updates, nil
		}
	}

	for {
		resp, err := w.watcher.Next(w.ctx)
		if err != nil {
			return []*naming.Update{}, err
		}

		if resp.Node.Dir {
			continue
		}

		updates := []*naming.Update{}

		switch resp.Action {
		case `set`, `update`, `create`:
			nodeData := NodeData{}
			err := json.Unmarshal([]byte(resp.Node.Value), &nodeData)
			if err != nil {
				grpclog.Println("Parse node data error:", err)
				continue
			}
			updates = append(updates, &naming.Update{
				Op:       naming.Add,
				Addr:     nodeData.Addr,
				Metadata: &nodeData.Metadata,
			})

		case `delete`, `expire`:
			nodeData := NodeData{}
			err := json.Unmarshal([]byte(resp.PrevNode.Value), &nodeData)
			if err != nil {
				grpclog.Println("Parse node data error:", err)
				continue
			}

			updates = append(updates, &naming.Update{
				Op:   naming.Delete,
				Addr: nodeData.Addr,
			})
		}
		return updates, nil

	}

	return []*naming.Update{}, nil
}
