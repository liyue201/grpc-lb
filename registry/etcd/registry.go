package etcd

import (
	"encoding/json"
	etcd "github.com/coreos/etcd/client"
	"golang.org/x/net/context"
	"google.golang.org/grpc/grpclog"
	"time"
)

type EtcdReigistry struct {
	keyapi etcd.KeysAPI
	key    string
	value  string
	ttl    time.Duration
	ctx    context.Context
	cancel context.CancelFunc
}

type Option struct {
	EtcdConfig  etcd.Config
	RegistryDir string
	ServiceName string
	NodeID      string
	NData       NodeData
	Ttl         time.Duration
}

type NodeData struct {
	Addr     string
	Metadata map[string]string
}

func NewRegistry(option Option) (*EtcdReigistry, error) {
	client, err := etcd.New(option.EtcdConfig)
	if err != nil {
		return nil, err
	}
	keyapi := etcd.NewKeysAPI(client)

	val, err := json.Marshal(option.NData)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	registry := &EtcdReigistry{
		keyapi: keyapi,
		key:    option.RegistryDir + "/" + option.ServiceName + "/" + option.NodeID,
		value:  string(val),
		ttl:    option.Ttl,
		ctx:    ctx,
		cancel: cancel,
	}
	return registry, nil
}

func (e *EtcdReigistry) Register() error {

	insertFunc := func() error {
		_, err := e.keyapi.Get(e.ctx, e.key, &etcd.GetOptions{Recursive: true})
		if err != nil {
			setopt := &etcd.SetOptions{TTL: e.ttl, PrevExist: etcd.PrevIgnore}
			if _, err := e.keyapi.Set(e.ctx, e.key, e.value, setopt); err != nil {
				grpclog.Printf("etcd: set service '%s' ttl to etcd error: %s\n", e.key, err.Error())
				return err
			}
		} else {
			// refresh set to true for not notifying the watcher
			setopt := &etcd.SetOptions{TTL: e.ttl, PrevExist: etcd.PrevExist, Refresh: true}
			if _, err := e.keyapi.Set(e.ctx, e.key, "", setopt); err != nil {
				grpclog.Printf("etcd: set service '%s' ttl to etcd error: %s\n", e.key, err.Error())
				return err
			}
		}
		return nil
	}

	err := insertFunc()
	if err != nil {
		return err
	}

	ticker := time.NewTicker(e.ttl / 5)
	for {
		select {
		case <-ticker.C:
			insertFunc()
		case <-e.ctx.Done():
			ticker.Stop()
			e.keyapi.Delete(context.Background(), e.key, &etcd.DeleteOptions{Recursive: true})
			return nil
		}
	}

	return nil
}

func (e *EtcdReigistry) Deregister() error {
	e.cancel()
	return nil
}
