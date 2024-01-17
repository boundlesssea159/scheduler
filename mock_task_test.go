package scheduler

import (
	"errors"
	"fmt"
	"time"
)

var (
	NormalRunTime  = 30
	LongRunTime    = 1000
	TaskNum        = 20
	ConcurrenceNum = 5
	FailError      = errors.New("fail task")
)

type SuccessTask struct {
}

func (this *SuccessTask) GetBizLogic() func() (bool, error) {
	return this.success
}

func (this *SuccessTask) success() (bool, error) {
	time.Sleep(time.Duration(NormalRunTime) * time.Millisecond)
	return true, nil
}

func (this *SuccessTask) Stop() (bool, error) {
	fmt.Println("success task Stop")
	return true, nil
}

func (this *SuccessTask) Resume() (bool, error) {
	fmt.Println("success task resume")
	return true, nil
}

func (this *SuccessTask) Cancel() (bool, error) {
	fmt.Println("success task cancel")
	return true, nil
}

func (this *SuccessTask) Pause() (bool, error) {
	fmt.Println("success task pause")
	return false, nil
}

func (this *SuccessTask) Delete() (bool, error) {
	fmt.Println("success task delete")
	return false, nil
}

type FailTask struct {
}

func (this *FailTask) GetBizLogic() func() (bool, error) {
	return this.fail
}

func (this *FailTask) fail() (bool, error) {
	time.Sleep(time.Duration(NormalRunTime) * time.Millisecond)
	return false, FailError
}

func (this *FailTask) Stop() (bool, error) {
	fmt.Println("fail task Stop")
	return false, FailError
}

func (this *FailTask) Resume() (bool, error) {
	fmt.Println("fail task resume")
	return false, FailError
}

func (this *FailTask) Cancel() (bool, error) {
	fmt.Println("fail task cancel")
	return false, FailError
}

func (this *FailTask) Pause() (bool, error) {
	fmt.Println("fail task pause")
	return false, FailError
}

func (this *FailTask) Delete() (bool, error) {
	fmt.Println("fail task delete")
	return false, FailError
}

type FailWithNilErrorTask struct {
	FailTask
}

func (this *FailWithNilErrorTask) GetBizLogic() func() (bool, error) {
	return this.notOk
}

func (this *FailWithNilErrorTask) notOk() (bool, error) {
	return false, nil
}

type LongTimeTask struct {
	properties map[string]string
}

func (this *LongTimeTask) GetBizLogic() func() (bool, error) {
	return this.longTime
}

func (this *LongTimeTask) longTime() (bool, error) {
	time.Sleep(time.Duration(LongRunTime) * time.Millisecond)
	return true, nil
}

func (this *LongTimeTask) Stop() (bool, error) {
	fmt.Println("long time task Stop")
	this.properties["property"] = "delete"
	return false, FailError
}

func (this *LongTimeTask) Resume() (bool, error) {
	fmt.Println("long time task resume")
	this.properties["property"] = "delete"
	return false, FailError
}

func (this *LongTimeTask) Cancel() (bool, error) {
	fmt.Println("long time task cancel")
	this.properties["property"] = "delete"
	return false, FailError
}

func (this *LongTimeTask) Pause() (bool, error) {
	fmt.Println("long task pause")
	this.properties["property"] = "delete"
	return false, FailError
}

func (this *LongTimeTask) Delete() (bool, error) {
	fmt.Println("long task delete")
	this.properties["property"] = "delete"
	return false, FailError
}

type PanicTask struct {
}

func (this *PanicTask) GetBizLogic() func() (bool, error) {
	return this.panic
}

func (this *PanicTask) panic() (bool, error) {
	panic("task panic")
}

func (this *PanicTask) Stop() (bool, error) {
	panic("task panic")
}

func (this *PanicTask) Resume() (bool, error) {
	panic("task panic")
}

func (this *PanicTask) Cancel() (bool, error) {
	panic("task panic")
}

func (this *PanicTask) Pause() (bool, error) {
	panic("task panic")
}

func (this *PanicTask) Delete() (bool, error) {
	panic("task panic")
}

type DoFuncPanicTask struct {
	PanicTask
}

func (this *DoFuncPanicTask) GetBizLogic() func() (bool, error) {
	return this.ok
}

func (this *DoFuncPanicTask) ok() (bool, error) {
	return true, nil
}
