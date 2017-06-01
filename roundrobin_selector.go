package grpclb

import (
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type RoundRobinSelector struct {
	baseSelector
	next int
}

func NewRoundRobinSelector() Selector {
	return &RoundRobinSelector{next: 0}
}

func (r *RoundRobinSelector) Get(ctx context.Context) (addr grpc.Address, err error) {
	if len(r.addrs) == 0 {
		err = AddrListEmptyErr
		return
	}

	if r.next >= len(r.addrs) {
		r.next = 0
	}
	next := r.next
	for {
		a := r.addrs[next]
		next = (next + 1) % len(r.addrs)
		if a.connected {
			addr = a.addr
			r.next = next
			return
		}
		if next == r.next {
			// Has iterated all the possible address but none is connected.
			addr = a.addr
			r.next = next
			return
		}
	}
}
