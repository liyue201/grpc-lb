
package etcd

import (
	etcd_cli "github.com/coreos/etcd/client"
	"google.golang.org/grpc/resolver"
	"sync"
)

var RegistryDir = "/grpclb"

type etcdResolver struct{
	scheme string
	etcdConfig etcd_cli.Config
	etcdWatchPath 	string
	watcher *Watcher
	target resolver.Target
	cc     resolver.ClientConn
	wg sync.WaitGroup
}

func (r *etcdResolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOption) (resolver.Resolver, error) {
	etcdCli, err := etcd_cli.New(r.etcdConfig)
	if err != nil{
		return  nil, err
	}
	r.target = target
	r.cc = cc
	r.watcher = newWatcher(r.etcdWatchPath, etcdCli)
	r.start()
	return r, nil
}

func (r *etcdResolver) Scheme() string {
	return r.scheme
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

func RegisterResolver(scheme string, etcdConfig etcd_cli.Config, srvName, srvVersion string)  {
	resolver.Register(&etcdResolver{
		scheme: scheme,
		etcdConfig: etcdConfig,
		etcdWatchPath: RegistryDir + "/" + srvName + "/" + srvVersion,
	})
}