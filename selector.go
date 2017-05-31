package grpclb

import (
	"errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type Selector interface {
	Add(addr grpc.Address) error
	Delete(addr grpc.Address) error
	Up(addr grpc.Address) (cnt int, connected bool)
	Down(addr grpc.Address) error
	AddrList() []*AddrInfo
	Get(ctx context.Context) (grpc.Address, error)
}

var AddrListEmptyErr = errors.New("addr list is emtpy")
var AddrExistErr = errors.New("addr exist")
var AddrDoseNotExistErr = errors.New("addr does not exist")

type baseSelector struct {
	addrs []*AddrInfo // all the addresses the client should potentially connect
}

func (b *baseSelector) Add(addr grpc.Address) error {
	for _, v := range b.addrs {
		if addr == v.addr {
			return AddrExistErr
		}
	}
	b.addrs = append(b.addrs, &AddrInfo{addr: addr})
	return nil
}

func (b *baseSelector) Delete(addr grpc.Address) error {
	for i, v := range b.addrs {
		if addr == v.addr {
			copy(b.addrs[i:], b.addrs[i+1:])
			b.addrs = b.addrs[:len(b.addrs)-1]
			return nil
		}
	}
	return AddrDoseNotExistErr
}

func (b *baseSelector) Up(addr grpc.Address) (cnt int, connected bool) {
	for _, a := range b.addrs {
		if a.addr == addr {
			if a.connected {
				return cnt, true
			}
			a.connected = true
		}
		if a.connected {
			cnt++
		}
	}
	return cnt, false
}

func (b *baseSelector) Down(addr grpc.Address) error {
	for _, a := range b.addrs {
		if addr == a.addr {
			a.connected = false
			break
		}
	}
	return nil
}

func (b *baseSelector) AddrList() []*AddrInfo {
	return b.addrs
}

func (b *baseSelector) Get(ctx context.Context) (addr grpc.Address, err error) {
	return
}
