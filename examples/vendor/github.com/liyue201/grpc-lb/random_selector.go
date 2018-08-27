package grpclb

import (
	"errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"math/rand"
	"time"
)

type RandomSelector struct {
	baseSelector
	r *rand.Rand
}

func NewRandomSelector() Selector {
	return &RandomSelector{
		r:            rand.New(rand.NewSource(time.Now().UnixNano())),
		baseSelector: baseSelector{addrMap: make(map[string]*AddrInfo)},
	}
}

func (r *RandomSelector) Get(ctx context.Context) (addr grpc.Address, err error) {
	if len(r.addrs) == 0 {
		return addr, errors.New("addr list is emtpy")
	}

	size := len(r.addrs)
	idx := r.r.Int() % size

	for i := 0; i < size; i++ {
		addr := r.addrs[(idx+i)%size]
		if addrInfo, ok := r.addrMap[addr]; ok {
			if addrInfo.connected {
				addrInfo.load++
				return addrInfo.addr, nil
			}
		}
	}
	return addr, NoAvailableAddressErr
}
