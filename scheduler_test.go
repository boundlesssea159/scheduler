package scheduler

import (
	"context"
	"errors"
	"fmt"
	"github.com/stretchr/testify/suite"
	_ "runtime/pprof"
	"strconv"
	"sync"
	"testing"
	"time"
)

var (
	NormalRunTime  = 30
	LongRunTime    = 70
	TaskNum        = 20
	ConcurrenceNum = 5
	FailError      = errors.New("fail task")
)

type SchedulerTest struct {
	suite.Suite
	scheduler *Scheduler
}

func (this *SchedulerTest) SetupTest() {
}

func (this *SchedulerTest) BeforeTest(suiteName, testName string) {
	this.scheduler, _ = NewScheduler(100, nil)
}

func (this *SchedulerTest) AfterTest(suiteName, testName string) {
	this.scheduler = nil
}

func buildSuccessTasks() []*Task[TaskIface] {
	return buildTasks(func(i int) *Task[TaskIface] {
		return NewTask[TaskIface](&SuccessTask{}, strconv.Itoa(i), i)
	})
}

func buildFailTasks() []*Task[TaskIface] {
	return buildTasks(func(i int) *Task[TaskIface] {
		return NewTask[TaskIface](&FailTask{}, strconv.Itoa(i), i)
	})
}

func buildLongTimeTasks() []*Task[TaskIface] {
	return buildTasks(func(i int) *Task[TaskIface] {
		return NewTask[TaskIface](&LongTimeTask{}, strconv.Itoa(i), i)
	})
}

func buildMixTasks() []*Task[TaskIface] {
	return buildTasks(func(i int) *Task[TaskIface] {
		if i%2 != 0 {
			return NewTask[TaskIface](&SuccessTask{}, strconv.Itoa(i), i)
		}
		return NewTask[TaskIface](&FailTask{}, strconv.Itoa(i), i)
	})
}

func buildTasks(f func(i int) *Task[TaskIface]) []*Task[TaskIface] {
	tasks := make([]*Task[TaskIface], 0, TaskNum)
	for i := TaskNum; i >= 1; i-- {
		tasks = append(tasks, f(i))
	}
	return tasks
}

func buildOrders(n int) []int {
	result := make([]int, 0, n)
	for i := 1; i <= n; i++ {
		result = append(result, i)
	}
	return result
}

func (this *SchedulerTest) concurrence(f func(i int)) {
	wg := sync.WaitGroup{}
	for i := 1; i <= ConcurrenceNum; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			f(i)
		}(i)
	}
	wg.Wait()
}

func (this *SchedulerTest) Test_ShouldReturnErrorIfSchedulerParamsInvalid() {

	_, err := NewScheduler(0, &LimiterParams{
		Ctx:       context.Background(),
		TokenRate: 1.0,
		Capacity:  1,
	})
	this.Assert().NotNil(err)

	_, err = NewScheduler(1, &LimiterParams{
		TokenRate: 1.0,
		Capacity:  1,
	})
	this.Assert().NotNil(err)

	_, err = NewScheduler(1, &LimiterParams{
		Ctx:       context.Background(),
		TokenRate: -0.1,
		Capacity:  1,
	})
	this.Assert().NotNil(err)

	_, err = NewScheduler(1, &LimiterParams{
		Ctx:       context.Background(),
		TokenRate: 1,
		Capacity:  -1,
	})
	this.Assert().NotNil(err)

	_, err = NewScheduler(-1, nil)
	this.Assert().NotNil(err)
}

func (this *SchedulerTest) TestExecuteByOrder_TaskShouldNotBeDuplicated() {
	this.TaskShouldNotBeDuplicated(this.scheduler.ExecuteByOrder)
}

func (this *SchedulerTest) TaskShouldNotBeDuplicated(fc func(batchId string, tasks Tasks) (*Waiter, error)) {
	n := 10
	wg := sync.WaitGroup{}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := fc("1", buildLongTimeTasks())
			if err != nil {
				this.Assert().True(this.scheduler.IsSubmitted("1"))
			}
		}()
	}
	wg.Wait()
}

func (this *SchedulerTest) TestExecuteByOrder_TaskShouldRunByOrder() {
	this.concurrence(func(i int) {
		this.runByOrder(i)
	})
}

func (this *SchedulerTest) runByOrder(i int) {
	successTasks := buildSuccessTasks()
	batchId := strconv.Itoa(i)
	wait, err := this.scheduler.ExecuteByOrder(batchId, successTasks)
	this.Assert().Nil(err)
	this.Assert().NotNil(wait)
	wait.Wait()
	this.Assert().Equal(buildOrders(len(successTasks)), wait.GetOrders())
	this.Assert().False(this.scheduler.IsSubmitted(batchId))
}

func (this *SchedulerTest) TestExecuteByOrder_ShouldReturnErrorIfTaskError() {
	this.concurrence(func(i int) {
		batchId := strconv.Itoa(i)
		wait, err := this.scheduler.ExecuteByOrder(batchId, buildFailTasks())
		this.Assert().Nil(err)
		wait.Wait()
		this.Assert().NotNil(wait.ResultErr())
		this.Assert().False(this.scheduler.IsSubmitted(strconv.Itoa(i)))
		// retry after fail, should success
		this.runByOrder(i)
	})
}

func (this *SchedulerTest) TestExecuteByOrder_TaskShouldBeControlledByTokenRate() {
	this.concurrence(func(i int) {
		// firstly, test fast rate
		firstCost := this.apiCost(i, 100, 100, func(scheduler *Scheduler, i int, tasks Tasks) (*Waiter, error) {
			return scheduler.ExecuteByOrder(strconv.Itoa(i), tasks)
		}, buildSuccessTasks())
		// secondly, test slow rate
		secondCost := this.apiCost(i, 100, 5, func(scheduler *Scheduler, i int, tasks Tasks) (*Waiter, error) {
			return scheduler.ExecuteByOrder(strconv.Itoa(i), tasks)
		}, buildSuccessTasks())
		this.Assert().Less(firstCost, secondCost)
		fmt.Printf("TestExecuteByOrder_TaskShouldBeControlledByTokenRate first cost:%+v , second cost:%+v \n", firstCost, secondCost)
	})
}

func (this *SchedulerTest) apiCost(i, capacity int, rate float64, f func(scheduler *Scheduler, i int, tasks Tasks) (*Waiter, error), tasks []*Task[TaskIface]) int64 {
	scheduler, _ := NewScheduler(capacity, &LimiterParams{
		Ctx:       context.Background(),
		TokenRate: rate,
		Capacity:  1,
	})
	startTime := time.Now()
	wait, err := f(scheduler, i, tasks)
	this.Assert().Nil(err)
	wait.Wait()
	return time.Now().Sub(startTime).Milliseconds()
}

func (this *SchedulerTest) TestExecuteByOrder_ShouldReturnTokenTimeOutErrorIfWaitTimeOut() {
	this.concurrence(func(i int) {
		batchId := strconv.Itoa(i)
		ctx, _ := context.WithTimeout(context.Background(), 50*time.Millisecond)
		scheduler, _ := NewScheduler(100, &LimiterParams{
			Ctx:       ctx,
			TokenRate: 1,
			Capacity:  0,
		})
		waiter, err := scheduler.ExecuteByOrder(batchId, buildSuccessTasks())
		//this.Assert().Equal(TokenTimeoutError, err)
		this.Assert().Nil(err)
		waiter.Wait()
		this.Assert().Equal(TokenTimeoutError, waiter.ResultErr())
	})
}

func (this *SchedulerTest) TestExecuteByOrder_ConcurrenceNumShouldBeLimited() {
	this.concurrence(func(i int) {
		f := func(i int, capacity int) int64 {
			batchId := strconv.Itoa(i)
			scheduler, _ := NewScheduler(capacity, &LimiterParams{
				Ctx:       context.Background(),
				TokenRate: 100,
				Capacity:  100,
			})
			starTime := time.Now()
			// firstly, let concurrency goruntine to occupy capacity
			_, errOne := scheduler.ExecuteByConcurrency(strconv.Itoa(i+1), buildSuccessTasks())
			this.Assert().Nil(errOne)
			// secondly, verify block
			waiter, err := scheduler.ExecuteByOrder(batchId, buildSuccessTasks())
			this.Assert().Nil(err)
			waiter.Wait()
			sub := time.Now().Sub(starTime).Milliseconds()
			this.Assert().Less(int64(TaskNum*NormalRunTime), sub)
			return sub
		}
		// capacity similar to infinity
		firstCost := f(i, 1000)
		// capacity is limited
		secondCost := f(i, 1)
		this.Assert().Less(firstCost, secondCost)
		fmt.Printf("TestExecuteByOrder_ConcurrenceNumShouldBeLimited first cost:%+v second cost:%+v \n", firstCost, secondCost)
	})
}

func (this *SchedulerTest) TestExecuteByConcurrency_ShouldBeAsync() {
	batchId := strconv.Itoa(1)
	startTime := time.Now()
	waiter, err := this.scheduler.ExecuteByConcurrency(batchId, buildLongTimeTasks())
	this.Assert().Nil(err)
	this.Assert().Less(time.Now().Sub(startTime).Milliseconds(), int64(LongRunTime))
	waiter.Wait()
	this.Assert().Equal(len(waiter.GetOrders()), TaskNum)
	this.Assert().Nil(waiter.ResultErr())
}

func (this *SchedulerTest) TestExecuteByConcurrency_TaskShouldNotBeDuplicated() {
	this.TaskShouldNotBeDuplicated(this.scheduler.ExecuteByConcurrency)
}

func (this *SchedulerTest) TestExecuteByConcurrency_TaskShouldRunByConcurrence() {
	this.concurrence(func(i int) {
		batchId := strconv.Itoa(i)
		waiter, err := this.scheduler.ExecuteByConcurrency(batchId, buildSuccessTasks())
		this.Assert().Nil(err)
		waiter.Wait()
		fmt.Println(len(waiter.GetOrders()), waiter.GetOrders())
		this.Assert().False(this.scheduler.IsSubmitted(batchId))
	})
}

func (this *SchedulerTest) TestExecuteByConcurrency_ShouldStopBehindTasksIfPreOneFail() {
	this.concurrence(func(i int) {
		batchId := strconv.Itoa(i)
		tasks := make(Tasks, 0)
		for i := 0; i < 50; i++ {
			tasks = append(tasks, buildMixTasks()...)
		}
		startTime := time.Now()
		waiter, err := this.scheduler.ExecuteByConcurrency(batchId, tasks)
		this.Assert().Nil(err)
		waiter.Wait()
		this.Assert().Equal(FailError, waiter.ResultErr())
		this.Assert().Less(time.Now().Sub(startTime).Milliseconds(), int64(TaskNum*NormalRunTime))
	})
}

func (this *SchedulerTest) Test_ShouldReturnErrorIfParamEmpty() {
	this.concurrence(func(i int) {
		_, err := this.scheduler.ExecuteByOrder("", buildSuccessTasks())
		this.Assert().Equal(InvalidParamError, err)
		_, err = this.scheduler.ExecuteByOrder("1", nil)
		this.Assert().Equal(InvalidParamError, err)
		_, err = this.scheduler.ExecuteByConcurrency("", buildSuccessTasks())
		this.Assert().Equal(InvalidParamError, err)
		_, err = this.scheduler.ExecuteByConcurrency("1", nil)
		this.Assert().Equal(InvalidParamError, err)
	})
}

func (this *SchedulerTest) TestExecuteByConcurrency_ShouldReturnTokenTimeOutErrorIfWaitTimeOut() {
	this.concurrence(func(i int) {
		batchId := strconv.Itoa(i)
		ctx, _ := context.WithTimeout(context.Background(), 50*time.Millisecond)
		scheduler, _ := NewScheduler(100, &LimiterParams{
			Ctx:       ctx,
			TokenRate: 1,
			Capacity:  0,
		})
		waiter, err := scheduler.ExecuteByConcurrency(batchId, buildSuccessTasks())
		this.Assert().Nil(err)
		waiter.Wait()
		this.Assert().Equal(TokenTimeoutError, waiter.ResultErr())
	})
}

func (this *SchedulerTest) TestExecuteByConcurrency_ConcurrenceNumShouldBeLimited() {
	this.concurrence(func(i int) {
		batchId := strconv.Itoa(i)
		scheduler, _ := NewScheduler(1, &LimiterParams{
			Ctx:       context.Background(),
			TokenRate: 100,
			Capacity:  100,
		})
		starTime := time.Now()
		waiter, err := scheduler.ExecuteByConcurrency(batchId, buildSuccessTasks())
		this.Assert().Nil(err)
		waiter.Wait()
		this.Assert().Less(int64(TaskNum*NormalRunTime), time.Now().Sub(starTime).Milliseconds()) // if serial cost less than total cost,it illustrates that concurrence num be limited
	})
}

func (this *SchedulerTest) TestExecuteByConcurrency_TaskShouldBeControlledByTokenRate() {
	this.concurrence(func(i int) {
		// firstly, test fast rate
		firstCost := this.apiCost(i, 100, 100, func(scheduler *Scheduler, i int, tasks Tasks) (*Waiter, error) {
			return scheduler.ExecuteByConcurrency(strconv.Itoa(i), tasks)
		}, buildSuccessTasks())
		// secondly, test slow rate
		secondCost := this.apiCost(i, 100, 5, func(scheduler *Scheduler, i int, tasks Tasks) (*Waiter, error) {
			return scheduler.ExecuteByConcurrency(strconv.Itoa(i), tasks)
		}, buildSuccessTasks())
		this.Assert().Less(firstCost, secondCost)
		fmt.Printf("TestExecuteByConcurrency_TaskShouldBeControlledByTokenRate first cost:%+v , second cost:%+v \n", firstCost, secondCost)
	})
}

func (this *SchedulerTest) Test_ShouldNotCrashIfTaskOccurPanic() {
	this.concurrence(func(i int) {
		waiter1, err1 := this.scheduler.ExecuteByOrder(strconv.Itoa(i), []*Task[TaskIface]{NewTask[TaskIface](&PanicTask{}, "", i)})
		this.Assert().Nil(err1)
		waiter1.Wait()
		this.Assert().NotNil(waiter1.ResultErr())

		waiter2, err2 := this.scheduler.ExecuteByConcurrency(strconv.Itoa(i), []*Task[TaskIface]{NewTask[TaskIface](&PanicTask{}, "", i)})
		this.Assert().Nil(err2)
		waiter2.Wait()
		this.Assert().NotNil(waiter2.ResultErr())
	})
}

func (this *SchedulerTest) Test_ShouldRunMuchBatchTasks() {
	scheduler, _ := NewScheduler(100000, nil)

	wg := sync.WaitGroup{}
	// success
	wg.Add(1)
	go func() {
		defer wg.Done()
		waiter, err := scheduler.ExecuteByConcurrency(strconv.Itoa(1), buildSuccessTasks())
		this.Assert().Nil(err)
		waiter.Wait()
		this.Assert().Nil(waiter.ResultErr())
	}()

	// fail
	wg.Add(1)
	go func() {
		defer wg.Done()
		waiter, err := scheduler.ExecuteByOrder(strconv.Itoa(2), buildFailTasks())
		this.Assert().Nil(err)
		waiter.Wait()
		this.Assert().NotNil(waiter.ResultErr())
	}()

	// success
	wg.Add(1)
	go func() {
		defer wg.Done()
		waiter, err := scheduler.ExecuteByOrder(strconv.Itoa(3), buildSuccessTasks())
		this.Assert().Nil(err)
		waiter.Wait()
		this.Assert().Nil(waiter.ResultErr())
	}()

	// fail
	wg.Add(1)
	go func() {
		defer wg.Done()
		waiter, err := scheduler.ExecuteByConcurrency(strconv.Itoa(4), buildMixTasks())
		this.Assert().Nil(err)
		waiter.Wait()
		this.Assert().NotNil(waiter.ResultErr())
	}()

	// success
	wg.Add(1)
	go func() {
		defer wg.Done()
		waiter, err := scheduler.ExecuteByConcurrency(strconv.Itoa(5), buildSuccessTasks())
		this.Assert().Nil(err)
		waiter.Wait()
		this.Assert().NotEmpty(waiter.GetOrders())
		this.Assert().Nil(waiter.ResultErr())
		fmt.Println(waiter.GetOrders())
	}()
	wg.Wait()
}

func (this *SchedulerTest) Test_ShouldBeAsync() {
	batchId1 := strconv.Itoa(1)
	startTime := time.Now()
	waiter1, err := this.scheduler.ExecuteByOrder(batchId1, buildSuccessTasks())
	this.Assert().Nil(err)
	batchId2 := strconv.Itoa(2)
	waiter2, err := this.scheduler.ExecuteByConcurrency(batchId2, buildSuccessTasks())
	this.Assert().Nil(err)
	this.Assert().Less(time.Now().Sub(startTime).Milliseconds(), int64(LongRunTime))
	waiter1.Wait()
	waiter2.Wait()
	this.Assert().Equal(len(waiter1.GetOrders()), TaskNum)
	this.Assert().Nil(waiter1.ResultErr())
	this.Assert().Equal(len(waiter2.GetOrders()), TaskNum)
	this.Assert().Nil(waiter2.ResultErr())
}

func Test_SchedulerTest(t *testing.T) {
	suite.Run(t, new(SchedulerTest))
}

type SuccessTask struct {
}

func (this *SuccessTask) GetBizLogic() func() error {
	return this.success
}

func (this *SuccessTask) success() error {
	time.Sleep(time.Duration(NormalRunTime) * time.Millisecond)
	return nil
}

type FailTask struct {
}

func (this *FailTask) GetBizLogic() func() error {
	return this.fail
}

func (this *FailTask) fail() error {
	time.Sleep(time.Duration(NormalRunTime) * time.Millisecond)
	return FailError
}

type LongTimeTask struct {
}

func (this *LongTimeTask) GetBizLogic() func() error {
	return this.longTime
}

func (this *LongTimeTask) longTime() error {
	time.Sleep(time.Duration(LongRunTime) * time.Millisecond)
	return nil
}

type PanicTask struct {
}

func (this *PanicTask) GetBizLogic() func() error {
	return this.panic
}

func (this *PanicTask) panic() error {
	panic("task panic")
	return nil
}
