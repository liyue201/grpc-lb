package etcd

import (
	"encoding/json"
	"fmt"
	etcd3 "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/etcdserver/api/v3rpc/rpctypes"
	"github.com/liyue201/grpc-lb/registry"
	"golang.org/x/net/context"
	"google.golang.org/grpc/grpclog"
	"sync"
	"time"
)

type Registrar struct {
	sync.RWMutex
	conf        *Config
	etcd3Client *etcd3.Client
	canceler    map[string]context.CancelFunc
}

type Config struct {
	EtcdConfig  etcd3.Config
	RegistryDir string
	Ttl         time.Duration
}

func NewRegistrar(conf *Config) (*Registrar, error) {
	client, err := etcd3.New(conf.EtcdConfig)
	if err != nil {
		return nil, err
	}

	registry := &Registrar{
		etcd3Client: client,
		conf:        conf,
		canceler:    make(map[string]context.CancelFunc),
	}
	return registry, nil
}

func (r *Registrar) Register(service *registry.ServiceInfo) error {
	val, err := json.Marshal(service)
	if err != nil {
		return err
	}

	key := r.conf.RegistryDir + "/" + service.Name + "/" + service.Version + "/" + service.InstanceId
	value := string(val)
	ctx, cancel := context.WithCancel(context.Background())
	r.Lock()
	r.canceler[service.InstanceId] = cancel
	r.Unlock()

	insertFunc := func() error {
		resp, err := r.etcd3Client.Grant(ctx, int64(r.conf.Ttl/time.Second))
		if err != nil {
			fmt.Printf("[Register] %v\n", err.Error())
			return err
		}
		_, err = r.etcd3Client.Get(ctx, key)
		if err != nil {
			if err == rpctypes.ErrKeyNotFound {
				if _, err := r.etcd3Client.Put(ctx, key, value, etcd3.WithLease(resp.ID)); err != nil {
					grpclog.Infof("grpclb: set key '%s' with ttl to etcd3 failed: %s", key, err.Error())
				}
			} else {
				grpclog.Infof("grpclb: key '%s' connect to etcd3 failed: %s", key, err.Error())
			}
			return err
		} else {
			// refresh set to true for not notifying the watcher
			if _, err := r.etcd3Client.Put(ctx, key, value, etcd3.WithLease(resp.ID)); err != nil {
				grpclog.Infof("grpclb: refresh key '%s' with ttl to etcd3 failed: %s", key, err.Error())
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
			if _, err := r.etcd3Client.Delete(context.Background(), key); err != nil {
				grpclog.Infof("grpclb: deregister '%s' failed: %s", key, err.Error())
			}
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
	r.etcd3Client.Close()
}
