package balancer

import (
	"github.com/liyue201/grpc-lb/common"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/grpclog"
	"math/rand"
	"sync"
	"time"
)

const Random = "random_x"

// newRandomBuilder creates a new random balancer builder.
func newRandomBuilder() balancer.Builder {
	return base.NewBalancerBuilder(Random, &randomPickerBuilder{}, base.Config{HealthCheck: true})
}

func init() {
	balancer.Register(newRandomBuilder())
}

type randomPickerBuilder struct{}

func (*randomPickerBuilder) Build(buildInfo base.PickerBuildInfo) balancer.Picker {
	grpclog.Infof("randomPicker: newPicker called with buildInfo: %v", buildInfo)
	if len(buildInfo.ReadySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}
	var scs []balancer.SubConn

	for subCon, subConnInfo := range buildInfo.ReadySCs {
		weight := common.GetWeight(subConnInfo.Address)
		for i := 0; i < weight; i++ {
			scs = append(scs, subCon)
		}
	}
	return &randomPicker{
		subConns: scs,
		rand:     rand.New(rand.NewSource(time.Now().Unix())),
	}
}

type randomPicker struct {
	subConns []balancer.SubConn
	mu       sync.Mutex
	rand     *rand.Rand
}

func (p *randomPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	ret := balancer.PickResult{}
	p.mu.Lock()
	ret.SubConn = p.subConns[p.rand.Intn(len(p.subConns))]
	p.mu.Unlock()
	return ret, nil
}
