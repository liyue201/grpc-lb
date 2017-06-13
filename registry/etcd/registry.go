package etcd

import (
	etcd "github.com/coreos/etcd/client"
	"golang.org/x/net/context"
	"log"
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
	NodeName    string
	NodeAddr    string
	Ttl         time.Duration
}

func NewRegistry(option Option) (*EtcdReigistry, error) {
	client, err := etcd.New(option.EtcdConfig)
	if err != nil {
		return nil, err
	}
	keyapi := etcd.NewKeysAPI(client)

	ctx, cancel := context.WithCancel(context.Background())
	registry := &EtcdReigistry{
		keyapi: keyapi,
		key:    option.RegistryDir + "/" + option.ServiceName + "/" + option.NodeName,
		value:  option.NodeAddr,
		ttl:    option.Ttl,
		ctx:    ctx,
		cancel: cancel,
	}
	return registry, nil
}

func (e *EtcdReigistry) Register() error {

	insertFunc := func() error {
		_, err := e.keyapi.Get(context.Background(), e.key, &etcd.GetOptions{Recursive: true})
		if err != nil {
			setopt := &etcd.SetOptions{TTL: e.ttl, PrevExist: etcd.PrevIgnore}
			if _, err := e.keyapi.Set(context.Background(), e.key, e.value, setopt); err != nil {
				log.Printf("etcd: set service '%s' ttl to etcd error: %s\n", e.key, err.Error())
				return err
			}
		} else {
			// refresh set to true for not notifying the watcher
			setopt := &etcd.SetOptions{TTL: e.ttl, PrevExist: etcd.PrevExist, Refresh: true}
			if _, err := e.keyapi.Set(context.Background(), e.key, "", setopt); err != nil {
				log.Printf("etcd: set service '%s' ttl to etcd error: %s\n", e.key, err.Error())
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
