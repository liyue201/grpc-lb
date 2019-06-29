package balancer

import (
	"context"
	"fmt"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/resolver"
	"strconv"
	"sync"
)

const ConsistanceHash = "consistance_hash"

var DefaultConsistanceHashKey = "consistance-hash"

func InitConsistanceHashBuilder(consistanceHashKey string) {
	balancer.Register(newConsistanceHashBuilder(consistanceHashKey))
}

// newConsistanceHashBuilder creates a new ConsistanceHash balancer builder.
func newConsistanceHashBuilder(consistanceHashKey string) balancer.Builder {
	return base.NewBalancerBuilderWithConfig(ConsistanceHash, &consistanceHashPickerBuilder{consistanceHashKey}, base.Config{HealthCheck: true})
}

type consistanceHashPickerBuilder struct {
	consistanceHashKey string
}

func (b *consistanceHashPickerBuilder) Build(readySCs map[resolver.Address]balancer.SubConn) balancer.Picker {
	grpclog.Infof("consistanceHashPicker: newPicker called with readySCs: %v", readySCs)
	if len(readySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}

	picker := &consistanceHashPicker{
		subConns:           make(map[string]balancer.SubConn),
		hash:               NewKetama(10, nil),
		consistanceHashKey: b.consistanceHashKey,
	}

	for addr, sc := range readySCs {
		weight := 1
		m, ok := addr.Metadata.(*map[string]string)
		w, ok := (*m)["weight"]
		if ok {
			n, err := strconv.Atoi(w)
			if err == nil && n > 0 {
				weight = n
			}
		}
		for i := 0; i < weight; i++ {
			node := wrapAddr(addr.Addr, i)
			picker.hash.Add(node)
			picker.subConns[node] = sc
		}
	}
	return picker
}

type consistanceHashPicker struct {
	subConns           map[string]balancer.SubConn
	hash               *Ketama
	consistanceHashKey string
	mu                 sync.Mutex
}

func (p *consistanceHashPicker) Pick(ctx context.Context, opts balancer.PickOptions) (balancer.SubConn, func(balancer.DoneInfo), error) {
	var sc balancer.SubConn
	p.mu.Lock()
	key, ok := ctx.Value(p.consistanceHashKey).(string)
	if ok {
		targetAddr, ok := p.hash.Get(key)
		if ok {
			sc = p.subConns[targetAddr]
		}
	}
	p.mu.Unlock()
	return sc, nil, nil
}

func wrapAddr(addr string, idx int) string {
	return fmt.Sprintf("%s-%d", addr, idx)
}
