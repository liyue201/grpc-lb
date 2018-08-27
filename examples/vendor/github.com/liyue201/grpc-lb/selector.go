package grpclb

import (
	"errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"strconv"
)

type Selector interface {
	Add(addr grpc.Address) error
	Delete(addr grpc.Address) error
	Up(addr grpc.Address) (cnt int, connected bool)
	Down(addr grpc.Address) error
	AddrList() []grpc.Address
	Get(ctx context.Context) (grpc.Address, error)
	Put(addr string) error
}

var AddrListEmptyErr = errors.New("addr list is emtpy")
var AddrExistErr = errors.New("addr exist")
var AddrDoesNotExistErr = errors.New("addr does not exist")
var NoAvailableAddressErr = errors.New("no available address")

type baseSelector struct {
	addrs   []string
	addrMap map[string]*AddrInfo
}

func (b *baseSelector) Add(addr grpc.Address) error {
	for _, v := range b.addrs {
		if addr.Addr == v {
			return AddrExistErr
		}
	}

	//fmt.Printf("Metadata = %#v\n", addr.Metadata)
	weight := 1
	m, ok := addr.Metadata.(*map[string]string)
	if ok {
		//fmt.Printf("m = %#v\n", m)
		w, ok := (*m)["weight"]
		if ok {
			n, err := strconv.Atoi(w)
			if err == nil && n > 0 {
				weight = n
			}
		}
	}

	b.addrMap[addr.Addr] = &AddrInfo{addr: addr, weight: weight, connected: false}

	for i := 0; i < weight; i++ {
		b.addrs = append(b.addrs, addr.Addr)
	}
	return nil
}

func (b *baseSelector) Delete(addr grpc.Address) error {

	firstIdx := -1
	lastIdx := -1
	for i, v := range b.addrs {
		if addr.Addr == v {
			if firstIdx == -1 {
				firstIdx = i
			}
			lastIdx = i
		} else {
			if lastIdx != -1 {
				break
			}
		}
	}
	if firstIdx >= 0 && lastIdx >= 0 {
		copy(b.addrs[firstIdx:], b.addrs[lastIdx+1:])
		b.addrs = b.addrs[:len(b.addrs)-(lastIdx-firstIdx+1)]
		delete(b.addrMap, addr.Addr)
		return nil
	}
	return AddrDoesNotExistErr
}

func (b *baseSelector) Up(addr grpc.Address) (cnt int, connected bool) {

	a, ok := b.addrMap[addr.Addr]
	if ok {
		if a.connected {
			return cnt, true
		}
		a.connected = true
	}
	for _, v := range b.addrMap {
		if v.connected {
			cnt++
			if cnt > 1 {
				break
			}
		}
	}
	return cnt, false
}

func (b *baseSelector) Down(addr grpc.Address) error {

	a, ok := b.addrMap[addr.Addr]
	if ok {
		a.connected = false
	}
	return nil
}

func (b *baseSelector) AddrList() []grpc.Address {
	list := []grpc.Address{}
	for _, v := range b.addrMap {
		list = append(list, v.addr)
	}
	return list
}

func (b *baseSelector) Get(ctx context.Context) (addr grpc.Address, err error) {
	return
}

func (b *baseSelector) Put(addr string) error {
	a, ok := b.addrMap[addr]
	if ok {
		a.load--
	}
	return nil
}
