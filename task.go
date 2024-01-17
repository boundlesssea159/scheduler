package scheduler

import (
	"errors"
	"fmt"
	"sync"
)

type TaskIface interface {
	GetBizLogic() func() (bool, error)
	Stop() (bool, error)
	Resume() (bool, error)
	Cancel() (bool, error)
	Pause() (bool, error)
	Delete() (bool, error)
}

type Task[T TaskIface] struct {
	id        string
	customer  T
	taskGroup *TaskGroup
	order     int
	mu        sync.Mutex
}

func NewTask[T TaskIface](customer T, id string, order int) *Task[T] {
	return &Task[T]{
		id:       id,
		customer: customer,
		order:    order,
	}
}

type Tasks []*Task[TaskIface]

func (this Tasks) Len() int {
	return len(this)
}

func (this Tasks) Less(i, j int) bool {
	return this[i].order <= this[j].order
}

func (this Tasks) Swap(i, j int) {
	this[i], this[j] = this[j], this[i]
}

func (this *Task[T]) setTaskGroup(taskGroup *TaskGroup) {
	this.taskGroup = taskGroup
}

func (this *Task[T]) run() (ok bool, err error) {
	defer func() {
		if rv := recover(); rv != nil {
			err = errors.New(fmt.Sprintf("task GetBizLogic() panic: %+v", rv))
			ok = false
		}
	}()
	ok, err = this.customer.GetBizLogic()()
	if err != nil || !ok {
		return false, err
	}
	this.taskGroup.recordId(this.id)
	return ok, err
}

func (this *Task[T]) do(do func() (bool, error)) (ok bool, err error) {
	defer func() {
		if rv := recover(); rv != nil {
			ok = false
			err = errors.New(fmt.Sprintf("task do() panic: %+v", rv))
		}
	}()
	this.mu.Lock()
	defer this.mu.Unlock()
	return do()
}

func (this *Task[T]) stop() (bool, error) {
	return this.customer.Stop()
}

func (this *Task[T]) resume() (bool, error) {
	return this.customer.Resume()
}

func (this *Task[T]) cancel() (bool, error) {
	return this.customer.Cancel()
}

func (this *Task[T]) pause() (bool, error) {
	return this.customer.Pause()
}

func (this *Task[T]) delete() (bool, error) {
	return this.customer.Delete()
}
