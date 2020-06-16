package etcd

import (
	"encoding/json"
	etcd_cli "github.com/coreos/etcd/client"
	"github.com/liyue201/grpc-lb/registry"
	"golang.org/x/net/context"
	"google.golang.org/grpc/grpclog"
	"sync"
	"time"
)

type Config struct {
	EtcdConfig  etcd_cli.Config
	RegistryDir string
	Ttl         time.Duration
}

type Registrar struct {
	sync.RWMutex
	conf     *Config
	keyapi   etcd_cli.KeysAPI
	canceler map[string]context.CancelFunc
}

func NewRegistrar(config *Config) (*Registrar, error) {
	client, err := etcd_cli.New(config.EtcdConfig)
	if err != nil {
		return nil, err
	}
	keyapi := etcd_cli.NewKeysAPI(client)
	registry := &Registrar{
		keyapi:   keyapi,
		conf:     config,
		canceler: make(map[string]context.CancelFunc),
	}
	return registry, nil
}

func (r *Registrar) Register(service *registry.ServiceInfo) error {
	val, err := json.Marshal(service)
	if err != nil {
		return err
	}
	value := string(val)
	ctx, cancel := context.WithCancel(context.Background())
	r.Lock()
	r.canceler[service.InstanceId] = cancel
	r.Unlock()

	key := r.conf.RegistryDir + "/" + service.Name + "/" + service.Version + "/" + service.InstanceId

	insertFunc := func() error {
		_, err := r.keyapi.Get(ctx, key, &etcd_cli.GetOptions{Recursive: true})
		if err != nil {
			setopt := &etcd_cli.SetOptions{TTL: r.conf.Ttl, PrevExist: etcd_cli.PrevIgnore}
			if _, err := r.keyapi.Set(ctx, key, value, setopt); err != nil {
				grpclog.Infof("etcd: set service '%s' ttl to etcd error: %s\n", key, err.Error())
				return err
			}
		} else {
			// refresh set to true for not notifying the watcher
			setopt := &etcd_cli.SetOptions{TTL: r.conf.Ttl, PrevExist: etcd_cli.PrevExist, Refresh: true}
			if _, err := r.keyapi.Set(ctx, key, "", setopt); err != nil {
				grpclog.Infof("etcd: set service '%s' ttl to etcd error: %s\n", key, err.Error())
				return err
			}
		}
		return nil
	}

	err = insertFunc()
	if err != nil {
		return err
	}

	ticker := time.NewTicker(r.conf.Ttl / 5)
	for {
		select {
		case <-ticker.C:
			insertFunc()
		case <-ctx.Done():
			ticker.Stop()
			r.keyapi.Delete(context.Background(), key, &etcd_cli.DeleteOptions{Recursive: true})
			return nil
		}
	}
	return nil
}

func (r *Registrar) Unregister(service *registry.ServiceInfo) error {
	r.RLock()
	cancel, ok := r.canceler[service.InstanceId]
	r.RUnlock()

	if ok {
		cancel()
	}
	return nil
}

func (r *Registrar) Close() {

}
