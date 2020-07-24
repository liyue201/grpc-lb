package balancer

import (
	"context"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/resolver"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const LeastConnection = "least_connection"

// newLeastConnectionBuilder creates a new leastConnection balancer builder.
func newLeastConnectionBuilder() balancer.Builder {
	return base.NewBalancerBuilderWithConfig(LeastConnection, &leastConnectionPickerBuilder{}, base.Config{HealthCheck: true})
}

func init() {
	balancer.Register(newLeastConnectionBuilder())
}

type leastConnectionPickerBuilder struct{}

func (*leastConnectionPickerBuilder) Build(readySCs map[resolver.Address]balancer.SubConn) balancer.Picker {
	grpclog.Infof("leastConnectionPicker: newPicker called with readySCs: %v", readySCs)
	if len(readySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}
	var nodes []*Node

	for _, sc := range readySCs {
		nodes = append(nodes, &Node{sc, 0})
	}

	return &leastConnectionPicker{
		nodes: nodes,
		rand:  rand.New(rand.NewSource(time.Now().Unix())),
	}
}

type Node struct {
	balancer.SubConn
	inflight int64
}

type leastConnectionPicker struct {
	nodes []*Node
	mu    sync.Mutex
	rand  *rand.Rand
}

func (p *leastConnectionPicker) Pick(ctx context.Context, opts balancer.PickOptions) (balancer.SubConn, func(balancer.DoneInfo), error) {
	if len(p.nodes) == 0 {
		return nil, nil, balancer.ErrNoSubConnAvailable
	}

	var node *Node
	if len(p.nodes) == 1 {
		node = p.nodes[0]
	} else {
		p.mu.Lock()
		a := p.rand.Intn(len(p.nodes))
		b := p.rand.Intn(len(p.nodes))
		p.mu.Unlock()
		if a == b {
			b = (b + 1) % len(p.nodes)
		}
		if p.nodes[a].inflight < p.nodes[b].inflight {
			node = p.nodes[a]
		} else {
			node = p.nodes[b]
		}
	}

	atomic.AddInt64(&node.inflight, 1)

	return node, func(info balancer.DoneInfo) {
		atomic.AddInt64(&node.inflight, -1)
	}, nil
}
