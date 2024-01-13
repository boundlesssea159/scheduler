package scheduler

import (
	"context"
	"sync"
)

type Scheduler struct {
	ctx         context.Context
	tasks       sync.Map
	rateLimiter *RateLimiter
	mu          sync.Mutex
	doMutex     sync.Mutex
	capacity    chan struct{}
}

func NewScheduler(capacity int, limiterParam *LimiterParams) (*Scheduler, error) {
	if capacity <= 0 || (limiterParam != nil && !limiterParam.IsValid()) {
		return nil, InvalidParamError
	}
	var (
		ctx         context.Context
		rateLimiter *RateLimiter
	)
	if limiterParam != nil {
		ctx = limiterParam.Ctx
		rateLimiter = NewRateLimiter(WithLimiterTokenRate(limiterParam.TokenRate), WithLimiterCapacity(limiterParam.Capacity))
	}
	return &Scheduler{
		ctx:         ctx,
		tasks:       sync.Map{},
		rateLimiter: rateLimiter,
		mu:          sync.Mutex{},
		doMutex:     sync.Mutex{},
		capacity:    make(chan struct{}, capacity),
	}, nil
}

func (this *Scheduler) check(batchId string, tasks Tasks) error {
	if err := this.shouldNotBeEmpty(batchId, tasks); err != nil {
		return err
	}
	if err := this.shouldBeUniq(batchId); err != nil {
		return err
	}
	return nil
}

func (this *Scheduler) shouldNotBeEmpty(batchId string, tasks Tasks) error {
	if batchId == "" || tasks == nil || tasks.Len() == 0 {
		return InvalidParamError
	}
	return nil
}

func (this *Scheduler) shouldBeUniq(batchId string) error {
	this.mu.Lock()
	defer this.mu.Unlock()
	_, ok := this.tasks.Load(batchId)
	if ok {
		return DuplicatedBatchTaskError
	}
	return nil
}

func (this *Scheduler) IsSubmitted(batchId string) bool {
	_, ok := this.tasks.Load(batchId)
	return ok
}

func (this *Scheduler) delete(batchId string) {
	this.tasks.Delete(batchId)
}

func (this *Scheduler) getToken(n int) error {
	if this.rateLimiter != nil {
		return this.rateLimiter.WaitN(this.ctx, n)
	}
	return nil
}

func (this *Scheduler) fillCapacity() {
	this.capacity <- struct{}{}
}

func (this *Scheduler) releaseCapacity() {
	<-this.capacity
}

func (this *Scheduler) ExecuteByOrder(batchId string, tasks Tasks) (*Waiter, error) {
	if err := this.check(batchId, tasks); err != nil {
		return nil, err
	}
	taskGroup := NewTaskGroup(batchId, tasks, this)
	this.tasks.Store(batchId, taskGroup)
	taskGroup.runByOrder()
	return taskGroup.getWaiter(), nil
}

func (this *Scheduler) ExecuteByConcurrency(batchId string, tasks Tasks) (*Waiter, error) {
	if err := this.check(batchId, tasks); err != nil {
		return nil, err
	}
	taskGroup := NewTaskGroup(batchId, tasks, this)
	this.tasks.Store(batchId, taskGroup)
	taskGroup.runByConcurrency()
	return taskGroup.getWaiter(), nil
}
