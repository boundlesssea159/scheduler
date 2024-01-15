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
	occupy      int32
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

func (this *Scheduler) occupyCapacity() error {
	select {
	case this.capacity <- struct{}{}:
		return nil
	default:
		return BlockingError
	}
}

func (this *Scheduler) releaseCapacity() {
	<-this.capacity
}

func (this *Scheduler) ExecuteByOrder(batchId string, tasks Tasks) (*Waiter, error) {
	return this.execute(func(taskGroup *TaskGroup) func() {
		return taskGroup.runByOrder
	}, batchId, tasks)
}

func (this *Scheduler) ExecuteByConcurrency(batchId string, tasks Tasks) (*Waiter, error) {
	return this.execute(func(taskGroup *TaskGroup) func() {
		return taskGroup.runByConcurrency
	}, batchId, tasks)
}

func (this *Scheduler) execute(executeFunc func(taskGroup *TaskGroup) func(), batchId string, tasks Tasks) (*Waiter, error) {
	if err := this.check(batchId, tasks); err != nil {
		return nil, err
	}
	taskGroup := NewTaskGroup(batchId, tasks, this)
	this.tasks.Store(batchId, taskGroup)
	executeFunc(taskGroup)()
	return taskGroup.getWaiter(), nil
}

func (this *Scheduler) Do(do func(group *TaskGroup, taskId string) (bool, error), batchId, taskId string) (bool, error) {
	this.doMutex.Lock()
	defer this.doMutex.Unlock()
	value, ok := this.tasks.Load(batchId)
	if ok {
		group := value.(*TaskGroup)
		return do(group, taskId)
	}
	return false, TaskNotFindError
}

func Stop(group *TaskGroup, taskId string) (bool, error) {
	return group.do(taskId, stop)
}

func Resume(group *TaskGroup, taskId string) (bool, error) {
	return group.do(taskId, resume)
}

func Cancel(group *TaskGroup, taskId string) (bool, error) {
	return group.do(taskId, cancel)
}

func Pause(group *TaskGroup, taskId string) (bool, error) {
	return group.do(taskId, pause)
}

func Delete(group *TaskGroup, taskId string) (bool, error) {
	return group.do(taskId, delete)
}
