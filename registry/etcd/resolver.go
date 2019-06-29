
package etcd

import (
	"fmt"
	"google.golang.org/grpc/resolver"
	etcd_cli "github.com/coreos/etcd/client"
	"sync"
)

const scheme = "etcd"
var RegistryDir = "/grpclb"
const EtcdTarget = "etcd:///test"

type etcdResolver struct{
	etcdConfig etcd_cli.Config
	etcdWatchPath 	string
	watcher *Watcher
	target resolver.Target
	cc     resolver.ClientConn
	wg sync.WaitGroup
}

func (r *etcdResolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOption) (resolver.Resolver, error) {
	fmt.Printf("etcdResolver [Build]\n")
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

func (*etcdResolver) Scheme() string {
	fmt.Printf("etcdResolver [Scheme]\n")
	return scheme
}

func (r *etcdResolver) start() {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		out := r.watcher.Watch()
		for addr := range out {
			fmt.Printf("[etcdResolver start] %v\n", addr)
			r.cc.UpdateState(resolver.State{Addresses: addr})
		}
	}()
}

func (r *etcdResolver) ResolveNow(o resolver.ResolveNowOption) {
	fmt.Printf("etcdResolver [ResolveNow]\n")
}

func (r *etcdResolver) Close() {
	fmt.Printf("etcdResolver [Close]\n")

	r.watcher.Close()
	r.wg.Wait()

	fmt.Printf("etcdResolver [Close] ok \n")
}

func InitEtcdResolver(etcdConfig etcd_cli.Config, srvName, srvVersion string)  {
	resolver.Register(&etcdResolver{
		etcdConfig: etcdConfig,
		etcdWatchPath: RegistryDir + "/" + srvName + "/" + srvVersion,
	})
}