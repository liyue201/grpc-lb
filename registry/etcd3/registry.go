package etcd

import (
	"encoding/json"
	"fmt"
	etcd3 "go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
	"golang.org/x/net/context"
	"google.golang.org/grpc/grpclog"
	"time"
)

type Registrar struct {
	etcd3Client *etcd3.Client
	key         string
	value       string
	ttl         time.Duration
	ctx         context.Context
	cancel      context.CancelFunc
}

type Option struct {
	EtcdConfig     etcd3.Config
	RegistryDir    string
	ServiceName    string
	ServiceServion string
	NodeID         string
	NData          NodeData
	Ttl            time.Duration
}

type NodeData struct {
	Addr     string
	Metadata map[string]string
}

func NewRegistrar(option Option) (*Registrar, error) {
	client, err := etcd3.New(option.EtcdConfig)
	if err != nil {
		return nil, err
	}

	val, err := json.Marshal(option.NData)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	registry := &Registrar{
		etcd3Client: client,
		key:         option.RegistryDir + "/" + option.ServiceName + "/" + option.ServiceServion + "/" + option.NodeID,
		value:       string(val),
		ttl:         option.Ttl / time.Second,
		ctx:         ctx,
		cancel:      cancel,
	}
	return registry, nil
}

func (e *Registrar) Register() error {

	insertFunc := func() error {
		resp, err := e.etcd3Client.Grant(e.ctx, int64(e.ttl))
		if err != nil {
			fmt.Printf("[Register] %v\n", err.Error())
			return err
		}
		_, err = e.etcd3Client.Get(e.ctx, e.key)
		if err != nil {
			if err == rpctypes.ErrKeyNotFound {
				if _, err := e.etcd3Client.Put(e.ctx, e.key, e.value, etcd3.WithLease(resp.ID)); err != nil {
					grpclog.Infof("grpclb: set key '%s' with ttl to etcd3 failed: %s", e.key, err.Error())
				}
			} else {
				grpclog.Infof("grpclb: key '%s' connect to etcd3 failed: %s", e.key, err.Error())
			}
			return err
		} else {
			// refresh set to true for not notifying the watcher
			if _, err := e.etcd3Client.Put(e.ctx, e.key, e.value, etcd3.WithLease(resp.ID)); err != nil {
				grpclog.Infof("grpclb: refresh key '%s' with ttl to etcd3 failed: %s", e.key, err.Error())
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
			if _, err := e.etcd3Client.Delete(context.Background(), e.key); err != nil {
				grpclog.Infof("grpclb: deregister '%s' failed: %s", e.key, err.Error())
			}
			return nil
		}
	}

	return nil
}

func (e *Registrar) Deregister() error {
	e.cancel()
	return nil
}
