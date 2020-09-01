package zk

import (
	"google.golang.org/grpc/resolver"
	"sync"
)

type zkResolver struct {
	scheme      string
	zkServers   []string
	zkWatchPath string
	watcher     *Watcher
	cc          resolver.ClientConn
	wg          sync.WaitGroup
}

func (r *zkResolver) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r.cc = cc
	var err error
	r.watcher, err = newWatcher(r.zkServers, r.zkWatchPath)
	if err != nil {
		return nil, err
	}
	r.start()
	return r, nil
}

func (r *zkResolver) Scheme() string {
	return r.scheme
}

func (r *zkResolver) start() {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		out := r.watcher.Watch()
		if out != nil {
			for addr := range out {
				r.cc.UpdateState(resolver.State{Addresses: addr})
			}
		}
	}()
}

func (r *zkResolver) ResolveNow(o resolver.ResolveNowOptions) {
}

func (r *zkResolver) Close() {
	r.watcher.Close()
	r.wg.Wait()
}

func RegisterResolver(scheme string, zkServers []string, registryDir, srvName, srvVersion string) {
	resolver.Register(&zkResolver{
		scheme:      scheme,
		zkServers:   zkServers,
		zkWatchPath: registryDir + "/" + srvName + "/" + srvVersion,
	})
}
