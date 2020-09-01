package balancer

import (
	"fmt"
	"github.com/liyue201/grpc-lb/common"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/grpclog"
)

const ConsistentHash = "consistent_hash_x"

var DefaultConsistentHashKey = "consistent-hash"

func InitConsistentHashBuilder(consistanceHashKey string) {
	balancer.Register(newConsistentHashBuilder(consistanceHashKey))
}

// newConsistanceHashBuilder creates a new ConsistanceHash balancer builder.
func newConsistentHashBuilder(consistentHashKey string) balancer.Builder {
	return base.NewBalancerBuilder(ConsistentHash, &consistentHashPickerBuilder{consistentHashKey}, base.Config{HealthCheck: true})
}

type consistentHashPickerBuilder struct {
	consistentHashKey string
}

func (b *consistentHashPickerBuilder) Build(buildInfo base.PickerBuildInfo) balancer.Picker {
	grpclog.Infof("consistentHashPicker: newPicker called with buildInfo: %v", buildInfo)
	if len(buildInfo.ReadySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}

	picker := &consistentHashPicker{
		subConns:          make(map[string]balancer.SubConn),
		hash:              NewKetama(10, nil),
		consistentHashKey: b.consistentHashKey,
	}

	for sc, conInfo := range buildInfo.ReadySCs {
		weight := common.GetWeight(conInfo.Address)
		for i := 0; i < weight; i++ {
			node := wrapAddr(conInfo.Address.Addr, i)
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
}

func (p *consistentHashPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	var ret balancer.PickResult
	key, ok := info.Ctx.Value(p.consistentHashKey).(string)
	if ok {
		targetAddr, ok := p.hash.Get(key)
		if ok {
			ret.SubConn = p.subConns[targetAddr]
		}
	}
	return ret, nil
}

func wrapAddr(addr string, idx int) string {
	return fmt.Sprintf("%s-%d", addr, idx)
}
