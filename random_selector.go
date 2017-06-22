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
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return &RandomSelector{r: r}
}

func (r *RandomSelector) Get(ctx context.Context) (addr grpc.Address, err error) {
	if len(r.addrs) == 0 {
		return addr, errors.New("addr list is emtpy")
	}

	size := len(r.addrs)
	idx := r.r.Int() % size

	for i := 0; i < size; i++ {
		if r.addrs[(idx+i)%size].connected {
			return r.addrs[(idx+i)%size].addr, nil
		}
	}
	return addr, NoAvailableAddressErr
}
