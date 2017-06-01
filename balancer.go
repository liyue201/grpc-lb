package grpclb

import (
	"fmt"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/naming"
	"sync"
)

var DefaultSelector = NewRandomSelector()

type AddrInfo struct {
	addr      grpc.Address
	connected bool
}

type balancer struct {
	r        naming.Resolver
	w        naming.Watcher
	selector Selector
	mu       sync.Mutex
	addrCh   chan []grpc.Address // the channel to notify gRPC internals the list of addresses the client should connect to.
	waitCh   chan struct{}       // the channel to block when there is no connected address available
	done     bool                // The Balancer is closed.
}

func NewBalancer(r naming.Resolver, selector Selector) grpc.Balancer {
	if selector == nil {
		selector = DefaultSelector
	}
	return &balancer{r: r, selector: selector}
}

func (b *balancer) watchAddrUpdates() error {
	updates, err := b.w.Next()
	if err != nil {
		grpclog.Printf("grpc: the naming watcher stops working due to %v.\n", err)
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, update := range updates {
		addr := grpc.Address{
			Addr:     update.Addr,
			Metadata: update.Metadata,
		}
		switch update.Op {
		case naming.Add:
			b.selector.Add(addr)
		case naming.Delete:
			b.selector.Delete(addr)
		default:
			grpclog.Println("Unknown update.Op ", update.Op)
		}
	}
	// Make a copy of b.addrs and write it onto b.addrCh so that gRPC internals gets notified.
	addrs := b.selector.AddrList()
	open := make([]grpc.Address, len(addrs))
	for i, v := range addrs {
		open[i] = v.addr
	}

	if b.done {
		return grpc.ErrClientConnClosing
	}
	b.addrCh <- open
	return nil
}

func (b *balancer) Start(target string, config grpc.BalancerConfig) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.done {
		return grpc.ErrClientConnClosing
	}
	if b.r == nil {
		// If there is no name resolver installed, it is not needed to
		// do name resolution. In this case, target is added into b.addrs
		// as the only address available and b.addrCh stays nil.

		//???
		//b.addrs = append(b.addrs, &AddrInfo{addr: grpc.Address{Addr: target}})
		return nil
	}
	w, err := b.r.Resolve(target)
	if err != nil {
		return err
	}
	b.w = w
	b.addrCh = make(chan []grpc.Address)
	go func() {
		for {
			if err := b.watchAddrUpdates(); err != nil {
				return
			}
		}
	}()
	return nil
}

// Up sets the connected state of addr and sends notification if there are pending
// Get() calls.
func (b *balancer) Up(addr grpc.Address) func(error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	cnt, connected := b.selector.Up(addr)
	if connected {
		return nil
	}

	// addr is only one which is connected. Notify the Get() callers who are blocking.
	if cnt == 1 && b.waitCh != nil {
		close(b.waitCh)
		b.waitCh = nil
	}
	return func(err error) {
		b.down(addr, err)
	}
}

// down unsets the connected state of addr.
func (b *balancer) down(addr grpc.Address, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.selector.Down(addr)
}

// Get returns the next addr in the rotation.
func (b *balancer) Get(ctx context.Context, opts grpc.BalancerGetOptions) (addr grpc.Address, put func(), err error) {
	var ch chan struct{}
	b.mu.Lock()
	if b.done {
		b.mu.Unlock()
		err = grpc.ErrClientConnClosing
		return
	}

	addr, err = b.selector.Get(ctx)
	if err == nil {
		b.mu.Unlock()
		return
	}
	if !opts.BlockingWait {
		err = fmt.Errorf("there is no address available")
		b.mu.Unlock()
		return
	}
	// Wait on b.waitCh for non-failfast RPCs.
	if b.waitCh == nil {
		ch = make(chan struct{})
		b.waitCh = ch
	} else {
		ch = b.waitCh
	}
	b.mu.Unlock()
	for {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		case <-ch:
			b.mu.Lock()
			if b.done {
				b.mu.Unlock()
				err = grpc.ErrClientConnClosing
				return
			}

			addr, err = b.selector.Get(ctx)
			if err == nil {
				b.mu.Unlock()
				return
			}

			// The newly added addr got removed by Down() again.
			if b.waitCh == nil {
				ch = make(chan struct{})
				b.waitCh = ch
			} else {
				ch = b.waitCh
			}
			b.mu.Unlock()
		}
	}
}

func (b *balancer) Notify() <-chan []grpc.Address {
	return b.addrCh
}

func (b *balancer) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.done = true
	if b.w != nil {
		b.w.Close()
	}
	if b.waitCh != nil {
		close(b.waitCh)
		b.waitCh = nil
	}
	if b.addrCh != nil {
		close(b.addrCh)
	}
	return nil
}
