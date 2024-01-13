package scheduler

import (
	"sync"
	"sync/atomic"
)

type Waiter struct {
	ok     chan struct{}
	closed int32
	orders []int
	err    atomic.Value
	mu     sync.Mutex
}

func (this *Waiter) Wait() {
	<-this.ok
}

func (this *Waiter) close() {
	close(this.ok)
}

func (this *Waiter) GetOrders() []int {
	return this.orders
}

func (this *Waiter) appendOrder(order int) {
	this.mu.Lock()
	defer this.mu.Unlock()
	this.orders = append(this.orders, order)
}

func (this *Waiter) ResultErr() error {
	if this.err.Load() == nil {
		return nil
	}
	return this.err.Load().(error)
}

func (this *Waiter) addErr(err error) {
	this.err.Store(err)
}
