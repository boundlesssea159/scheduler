package scheduler

import (
	"sort"
	"sync"
	"sync/atomic"
)

type TaskGroup struct {
	id        string
	scheduler *Scheduler
	tasks     Tasks
	waiter    *Waiter
	mu        sync.Mutex
}

func NewTaskGroup(id string, tasks Tasks, scheduler *Scheduler) *TaskGroup {
	return &TaskGroup{
		id:        id,
		scheduler: scheduler,
		tasks:     tasks,
		waiter: &Waiter{
			ok:     make(chan struct{}),
			orders: make([]int, 0, len(tasks)),
		},
	}
}

func (this *TaskGroup) sort() {
	sort.Sort(this.tasks)
}

func (this *TaskGroup) runByOrder() {
	go func() {
		this.sort()
		for _, task := range this.tasks {
			this.scheduler.fillCapacity()
			if err := this.scheduler.getToken(1); err != nil {
				this.scheduler.releaseCapacity()
				this.waiter.addErr(TokenTimeoutError)
				this.waiter.close()
				return
			}
			task.setTaskGroup(this)
			if err := task.run(); err != nil {
				this.waiter.addErr(err)
				this.waiter.close()
				this.scheduler.releaseCapacity()
				this.scheduler.delete(this.id)
				return
			}
			this.scheduler.releaseCapacity()
		}
	}()
}

func (this *TaskGroup) runByConcurrency() {
	go func() {
		var (
			wg  sync.WaitGroup
			Err atomic.Value
		)
		for _, task := range this.tasks {
			if Err.Load() != nil {
				break
			}
			this.scheduler.fillCapacity()
			if err := this.scheduler.getToken(1); err != nil {
				Err.Store(TokenTimeoutError)
				break
			}
			wg.Add(1)
			go func(task *Task[TaskIface]) {
				defer func() {
					wg.Done()
					this.scheduler.releaseCapacity()
				}()
				task.setTaskGroup(this)
				if err := task.run(); err != nil {
					Err.Store(err)
					this.scheduler.delete(this.id)
				}
			}(task)
		}
		wg.Wait()
		if Err.Load() != nil {
			this.waiter.addErr(Err.Load().(error))
			this.waiter.close()
		}
	}()
}

func (this *TaskGroup) getWaiter() *Waiter {
	return this.waiter
}

func (this *TaskGroup) recordOrder(order int) {
	this.waiter.appendOrder(order)
	if this.tasks.Len() == this.waiter.ordersLen() {
		this.scheduler.delete(this.id)
		this.waiter.close()
	}
}
