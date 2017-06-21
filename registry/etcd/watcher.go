package etcd

import (
	etcd "github.com/coreos/etcd/client"
	"golang.org/x/net/context"
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
			updates = append(updates, &naming.Update{
				Op:   naming.Add,
				Addr: n.Value,
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
			updates = append(updates, &naming.Update{
				Op:   naming.Add,
				Addr: resp.Node.Value,
			})
		case `delete`, `expire`:
			updates = append(updates, &naming.Update{
				Op:   naming.Delete,
				Addr: resp.PrevNode.Value,
			})
		}
		return updates, nil

	}

	return []*naming.Update{}, nil
}
