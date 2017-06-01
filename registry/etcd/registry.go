package etcd

import (
	"github.com/codinl/go-logger"
	etcd "github.com/coreos/etcd/client"
	"golang.org/x/net/context"
	"time"
)

type EtcdReigistry struct {
	keyapi etcd.KeysAPI
	key    string
	value  string
	ttl    time.Duration
	stop   chan struct{}
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

	registry := &EtcdReigistry{
		keyapi: keyapi,
		key:    option.RegistryDir + "/" + option.ServiceName + "/" + option.NodeName,
		value:  option.NodeAddr,
		ttl:    option.Ttl,
		stop:   make(chan struct{}),
	}
	return registry, nil
}

func (client *EtcdReigistry) Register() error {

	insertFunc := func() error {
		_, err := client.keyapi.Get(context.Background(), client.key, &etcd.GetOptions{Recursive: true})
		if err != nil {
			setopt := &etcd.SetOptions{TTL: client.ttl, PrevExist: etcd.PrevIgnore}
			if _, err := client.keyapi.Set(context.Background(), client.key, client.value, setopt); err != nil {
				logger.Errorf("etcd: set service '%s' ttl to etcd error: %s\n", client.key, err.Error())
				return err
			}
		} else {
			// refresh set to true for not notifying the watcher
			setopt := &etcd.SetOptions{TTL: client.ttl, PrevExist: etcd.PrevExist, Refresh: true}
			if _, err := client.keyapi.Set(context.Background(), client.key, "", setopt); err != nil {
				logger.Errorf("etcd: set service '%s' ttl to etcd error: %s\n", client.key, err.Error())
				return err
			}
		}
		return nil
	}

	err := insertFunc()
	if err != nil {
		return err
	}

	keepAliveTicker := time.NewTicker(client.ttl / 5)
	for {
		select {
		case <-keepAliveTicker.C:
			insertFunc()
		case <-client.stop:
			client.keyapi.Delete(context.Background(), client.key, &etcd.DeleteOptions{Recursive: true})
			return nil
		}
	}

	return nil
}

func (client *EtcdReigistry) Deregister() error {
	close(client.stop)
	return nil
}
