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
}

func NewTaskGroup(id string, tasks Tasks, scheduler *Scheduler) *TaskGroup {
	return &TaskGroup{
		id:        id,
		scheduler: scheduler,
		tasks:     tasks,
		waiter: &Waiter{
			ok:       make(chan struct{}),
			orderIds: make([]string, 0, len(tasks)),
		},
	}
}

func (this *TaskGroup) sort() {
	sort.Sort(this.tasks)
}

func (this *TaskGroup) runByOrder() {
	go func() {
		this.sort()
		var Err atomic.Value
		for _, task := range this.tasks {
			if Err.Load() != nil {
				break
			}
			err := this.scheduler.occupyCapacity()
			if err != nil {
				Err.Store(err)
				break
			}
			if err := this.scheduler.getToken(1); err != nil {
				this.scheduler.releaseCapacity()
				Err.Store(TokenTimeoutError)
				break
			}
			task.setTaskGroup(this)
			ok, err := task.run()
			if err != nil {
				Err.Store(err)
			} else if !ok {
				Err.Store(TerminationError)
			}
			this.scheduler.releaseCapacity()
		}
		this.over(Err)
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
			if err := this.scheduler.occupyCapacity(); err != nil {
				Err.Store(BlockingError)
				break
			}
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
				ok, err := task.run()
				if err != nil {
					Err.Store(err)
				} else if !ok {
					Err.Store(TerminationError)
				}
			}(task)
		}
		wg.Wait()
		this.over(Err)
	}()
}

func (this *TaskGroup) over(Err atomic.Value) {
	if Err.Load() != nil && Err.Load().(error) != TerminationError {
		this.waiter.addErr(Err.Load().(error))
	}
	this.scheduler.delete(this.id)
	this.waiter.close()
}

func (this *TaskGroup) getWaiter() *Waiter {
	return this.waiter
}

func (this *TaskGroup) recordId(id string) {
	this.waiter.appendId(id)
}

func (this *TaskGroup) do(id string, do func(task *Task[TaskIface]) (bool, error)) (bool, error) {
	for _, task := range this.tasks {
		if task.id == id {
			return do(task)
		}
	}
	return false, TaskNotFindError
}

func stop(task *Task[TaskIface]) (bool, error) {
	return task.stop()
}

func resume(task *Task[TaskIface]) (bool, error) {
	return task.resume()
}

func cancel(task *Task[TaskIface]) (bool, error) {
	return task.cancel()
}

func pause(task *Task[TaskIface]) (bool, error) {
	return task.pause()
}

func delete(task *Task[TaskIface]) (bool, error) {
	return task.delete()
}
