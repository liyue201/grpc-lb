package etcd

import (
	etcd_cli "go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc/resolver"
	"sync"
)

const scheme = "etcd3"
const EtcdTarget = "etcd3:///test"
var RegistryDir = "/grpclb"

type etcdResolver struct {
	etcdConfig    etcd_cli.Config
	etcdWatchPath string
	watcher       *Watcher
	cc            resolver.ClientConn
	wg            sync.WaitGroup
}

func (r *etcdResolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOption) (resolver.Resolver, error) {
	etcdCli, err := etcd_cli.New(r.etcdConfig)
	if err != nil {
		return nil, err
	}
	r.cc = cc
	r.watcher = newWatcher(r.etcdWatchPath, etcdCli)
	r.start()
	return r, nil
}

func (*etcdResolver) Scheme() string {
	return scheme
}

func (r *etcdResolver) start() {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		out := r.watcher.Watch()
		for addr := range out {
			r.cc.UpdateState(resolver.State{Addresses: addr})
		}
	}()
}

func (r *etcdResolver) ResolveNow(o resolver.ResolveNowOption) {
}

func (r *etcdResolver) Close() {
	r.watcher.Close()
	r.wg.Wait()
}

func InitEtcdResolver(etcdConfig etcd_cli.Config, srvName, srvVersion string) {
	resolver.Register(&etcdResolver{
		etcdConfig:    etcdConfig,
		etcdWatchPath: RegistryDir + "/" + srvName + "/" + srvVersion,
	})
}
