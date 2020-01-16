package etcd

import (
	"encoding/json"
	etcd_cli "github.com/coreos/etcd/client"
	"golang.org/x/net/context"
	"google.golang.org/grpc/grpclog"
	"time"
)

var RegistryDir = "/grpclb"

type Registrar struct {
	keyapi etcd_cli.KeysAPI
	key    string
	value  string
	ttl    time.Duration
	ctx    context.Context
	cancel context.CancelFunc
}

type Option struct {
	EtcdConfig     etcd_cli.Config
	RegistryDir    string
	ServiceName    string
	ServiceVersion string
	NodeID         string
	NData          NodeData
	Ttl            time.Duration
}

type NodeData struct {
	Addr     string
	Metadata map[string]string
}

func NewRegistrar(option Option) (*Registrar, error) {
	client, err := etcd_cli.New(option.EtcdConfig)
	if err != nil {
		return nil, err
	}
	keyapi := etcd_cli.NewKeysAPI(client)

	val, err := json.Marshal(option.NData)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	registry := &Registrar{
		keyapi: keyapi,
		key:    option.RegistryDir + "/" + option.ServiceName + "/" + option.ServiceVersion + "/" + option.NodeID,
		value:  string(val),
		ttl:    option.Ttl,
		ctx:    ctx,
		cancel: cancel,
	}
	return registry, nil
}

func (e *Registrar) Register() error {

	insertFunc := func() error {
		_, err := e.keyapi.Get(e.ctx, e.key, &etcd_cli.GetOptions{Recursive: true})
		if err != nil {
			setopt := &etcd_cli.SetOptions{TTL: e.ttl, PrevExist: etcd_cli.PrevIgnore}
			if _, err := e.keyapi.Set(e.ctx, e.key, e.value, setopt); err != nil {
				grpclog.Infof("etcd: set service '%s' ttl to etcd error: %s\n", e.key, err.Error())
				return err
			}
		} else {
			// refresh set to true for not notifying the watcher
			setopt := &etcd_cli.SetOptions{TTL: e.ttl, PrevExist: etcd_cli.PrevExist, Refresh: true}
			if _, err := e.keyapi.Set(e.ctx, e.key, "", setopt); err != nil {
				grpclog.Infof("etcd: set service '%s' ttl to etcd error: %s\n", e.key, err.Error())
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
			e.keyapi.Delete(context.Background(), e.key, &etcd_cli.DeleteOptions{Recursive: true})
			return nil
		}
	}

	return nil
}

func (e *Registrar) Unregister() error {
	e.cancel()
	return nil
}
