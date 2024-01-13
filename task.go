package scheduler

import (
	"errors"
	"fmt"
)

type TaskIface interface {
	GetBizLogic() func() error
}

type Task[T TaskIface] struct {
	id        string
	customer  T
	taskGroup *TaskGroup
	order     int
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

func (this *Task[T]) run() (err error) {
	defer func() {
		if rv := recover(); rv != nil {
			err = errors.New(fmt.Sprintf("task panic: %+v", rv))
		}
	}()
	if err = this.customer.GetBizLogic()(); err != nil {
		return err
	}
	this.taskGroup.recordOrder(this.order)
	return err
}
