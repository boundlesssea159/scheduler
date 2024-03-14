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
	return this.execute(func(taskGroup *TaskGroup) { taskGroup.runByOrder() }, batchId, tasks)
}

func (this *Scheduler) ExecuteByConcurrency(batchId string, tasks Tasks) (*Waiter, error) {
	return this.execute(func(taskGroup *TaskGroup) { taskGroup.runByConcurrency() }, batchId, tasks)
}

func (this *Scheduler) execute(do func(taskGroup *TaskGroup), batchId string, tasks Tasks) (*Waiter, error) {
	if err := this.check(batchId, tasks); err != nil {
		return nil, err
	}
	taskGroup := newTaskGroup(batchId, tasks, this)
	this.tasks.Store(batchId, taskGroup)
	do(taskGroup)
	return taskGroup.getWaiter(), nil
}

func (this *Scheduler) Do(do func(group *TaskGroup, taskId string) (bool, error), batchId, taskId string) (bool, error) {
	value, ok := this.tasks.Load(batchId)
	if ok {
		group := value.(*TaskGroup)
		return do(group, taskId)
	}
	return false, TaskNotFindError
}

func Stop(group *TaskGroup, taskId string) (bool, error) {
	return group.do(stop, taskId)
}

func Resume(group *TaskGroup, taskId string) (bool, error) {
	return group.do(resume, taskId)
}

func Cancel(group *TaskGroup, taskId string) (bool, error) {
	return group.do(cancel, taskId)
}

func Pause(group *TaskGroup, taskId string) (bool, error) {
	return group.do(pause, taskId)
}

func Delete(group *TaskGroup, taskId string) (bool, error) {
	return group.do(delete, taskId)
}
