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

const ConsistentHash = "consistent_hash"

var DefaultConsistentHashKey = "consistent-hash"

func InitConsistentHashBuilder(consistanceHashKey string) {
	balancer.Register(newConsistentHashBuilder(consistanceHashKey))
}

// newConsistanceHashBuilder creates a new ConsistanceHash balancer builder.
func newConsistentHashBuilder(consistentHashKey string) balancer.Builder {
	return base.NewBalancerBuilderWithConfig(ConsistentHash, &consistentHashPickerBuilder{consistentHashKey}, base.Config{HealthCheck: true})
}

type consistentHashPickerBuilder struct {
	consistentHashKey string
}

func (b *consistentHashPickerBuilder) Build(readySCs map[resolver.Address]balancer.SubConn) balancer.Picker {
	grpclog.Infof("consistentHashPicker: newPicker called with readySCs: %v", readySCs)
	if len(readySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}

	picker := &consistentHashPicker{
		subConns:          make(map[string]balancer.SubConn),
		hash:              NewKetama(10, nil),
		consistentHashKey: b.consistentHashKey,
	}

	for addr, sc := range readySCs {
		weight := 1
		if addr.Metadata != nil {
			if m, ok := addr.Metadata.(*map[string]string); ok {
				if w, ok := (*m)["weight"]; ok {
					n, err := strconv.Atoi(w)
					if err == nil && n > 0 {
						weight = n
					}
				}
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

type consistentHashPicker struct {
	subConns          map[string]balancer.SubConn
	hash              *Ketama
	consistentHashKey string
	mu                sync.Mutex
}

func (p *consistentHashPicker) Pick(ctx context.Context, opts balancer.PickOptions) (balancer.SubConn, func(balancer.DoneInfo), error) {
	var sc balancer.SubConn
	p.mu.Lock()
	key, ok := ctx.Value(p.consistentHashKey).(string)
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
