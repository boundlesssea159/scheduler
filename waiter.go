package scheduler

import (
	"sync"
	"sync/atomic"
)

type Waiter struct {
	ok       chan struct{}
	closed   int32
	orderIds []string
	err      atomic.Value
	mu       sync.RWMutex
}

func (this *Waiter) Wait() {
	<-this.ok
}

func (this *Waiter) close() {
	close(this.ok)
}

func (this *Waiter) GetTaskIds() []string {
	this.mu.RLock()
	defer this.mu.RUnlock()
	return this.orderIds
}

func (this *Waiter) appendId(id string) {
	this.mu.Lock()
	defer this.mu.Unlock()
	this.orderIds = append(this.orderIds, id)
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
