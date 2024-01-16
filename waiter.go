package scheduler

import (
	"sync"
	"sync/atomic"
)

type Waiter struct {
	ok      chan struct{}
	closed  int32
	taskIds []string
	err     atomic.Value
	mu      sync.RWMutex
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
	result := make([]string, 0, len(this.taskIds))
	for _, id := range this.taskIds {
		result = append(result, id)
	}
	return result
}

func (this *Waiter) appendId(id string) {
	this.mu.Lock()
	defer this.mu.Unlock()
	this.taskIds = append(this.taskIds, id)
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
