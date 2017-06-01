package grpclb

import (
	"errors"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

type KetamaSelector struct {
	baseSelector
	hash      *Ketama
	ketamaKey string
}

var (
	DefaultKetamaKey  = "grpc-lb-ketama-key"
	KetamaKeyEmptyErr = errors.New("ketama key is empty")
)

func NewKetamaSelector(ketamaKey string) Selector {
	if ketamaKey == "" {
		ketamaKey = DefaultKetamaKey
	}
	return &KetamaSelector{
		hash:      NewKetama(50, nil),
		ketamaKey: ketamaKey,
	}
}

func (s *KetamaSelector) Add(addr grpc.Address) error {
	err := s.baseSelector.Add(addr)
	if err == nil {
		s.hash.Add(addr.Addr)
	}
	return err
}

func (s *KetamaSelector) Delete(addr grpc.Address) error {
	err := s.baseSelector.Delete(addr)
	if err == nil {
		s.hash.Remove(addr.Addr)
	}
	return nil
}

func (s *KetamaSelector) Get(ctx context.Context) (addr grpc.Address, err error) {
	if len(s.addrs) == 0 {
		err = AddrListEmptyErr
		return
	}

	key, ok := ctx.Value(s.ketamaKey).(string)
	if ok {
		targetAddr, ok := s.hash.Get(key)
		if ok {
			for _, v := range s.addrs {
				if v.addr.Addr == targetAddr {
					addr = v.addr
					return
				}
			}
		} else {
			err = AddrDoseNotExistErr
		}
	} else {
		err = KetamaKeyEmptyErr
	}
	return
}
