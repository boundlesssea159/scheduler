package scheduler

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/suite"
	_ "runtime/pprof"
	"strconv"
	"sync"
	"testing"
	"time"
)

func Test_SchedulerTest(t *testing.T) {
	suite.Run(t, new(SchedulerTest))
}

type SchedulerTest struct {
	suite.Suite
	scheduler *Scheduler
}

func (this *SchedulerTest) BeforeTest(suiteName, testName string) {
	this.scheduler, _ = NewScheduler(100, nil)
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

func buildFalseTasks() []*Task[TaskIface] {
	return buildTasks(func(i int) *Task[TaskIface] {
		return NewTask[TaskIface](&FailWithNilErrorTask{}, strconv.Itoa(i), i)
	})
}

func buildLongTimeTasks() []*Task[TaskIface] {
	return buildTasks(func(i int) *Task[TaskIface] {
		return NewTask[TaskIface](&LongTimeTask{make(map[string]string)}, strconv.Itoa(i), i)
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

func concurrence(f func(i int)) {
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

func (this *SchedulerTest) Test_ShouldReturnErrorIfExecuteParamEmpty() {
	concurrence(func(i int) {
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
	concurrence(func(i int) {
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
	concurrence(func(i int) {
		batchId := strconv.Itoa(i)
		wait, err := this.scheduler.ExecuteByOrder(batchId, buildFailTasks())
		this.Assert().Nil(err)
		wait.Wait()
		this.Assert().NotNil(wait.ResultErr())
		this.Assert().False(this.scheduler.IsSubmitted(batchId))
		// retry after fail, should success
		this.runByOrder(i)
	})
}

func (this *SchedulerTest) TestExecuteByOrder_TaskShouldBeControlledByTokenRate() {
	concurrence(func(i int) {
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

func (this *SchedulerTest) TestExecuteByConcurrency_TaskShouldBeControlledByTokenRate() {
	concurrence(func(i int) {
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
	concurrence(func(i int) {
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

func (this *SchedulerTest) TestExecuteByOrder_ShouldStopIfPreOneError() {
	concurrence(func(i int) {
		batchId := strconv.Itoa(i)
		startTime := time.Now()
		waiter, err := this.scheduler.ExecuteByOrder(batchId, buildFailTasks())
		this.Assert().Nil(err)
		waiter.Wait()
		this.Assert().NotNil(waiter.ResultErr())
		this.Assert().Less(time.Now().Sub(startTime).Milliseconds(), int64(TaskNum*NormalRunTime))
	})
}

func (this *SchedulerTest) TestExecuteByOrder_ShouldStopIfPreOneFalse() {
	concurrence(func(i int) {
		batchId := strconv.Itoa(i)
		startTime := time.Now()
		waiter, err := this.scheduler.ExecuteByOrder(batchId, buildFalseTasks())
		this.Assert().Nil(err)
		waiter.Wait()
		this.Assert().Nil(waiter.ResultErr())
		this.Assert().Less(time.Now().Sub(startTime).Milliseconds(), int64(TaskNum*NormalRunTime))
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
	concurrence(func(i int) {
		batchId := strconv.Itoa(i)
		waiter, err := this.scheduler.ExecuteByConcurrency(batchId, buildSuccessTasks())
		this.Assert().Nil(err)
		waiter.Wait()
		fmt.Println(len(waiter.GetOrders()), waiter.GetOrders())
		this.Assert().False(this.scheduler.IsSubmitted(batchId))
	})
}

func (this *SchedulerTest) TestExecuteByConcurrency_ShouldStopBehindTasksIfPreOneFalse() {
	concurrence(func(i int) {
		batchId := strconv.Itoa(i)
		startTime := time.Now()
		waiter, err := this.scheduler.ExecuteByConcurrency(batchId, buildFalseTasks())
		this.Assert().Nil(err)
		waiter.Wait()
		this.Assert().Nil(waiter.ResultErr())
		this.Assert().Less(time.Now().Sub(startTime).Milliseconds(), int64(TaskNum*NormalRunTime))
	})
}

func (this *SchedulerTest) TestExecuteByConcurrency_ShouldReturnTokenTimeOutErrorIfWaitTimeOut() {
	concurrence(func(i int) {
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

func (this *SchedulerTest) TestExecuteByConcurrency_ShouldReturnErrorIfBlock() {
	scheduler, _ := NewScheduler(1, &LimiterParams{
		Ctx:       context.Background(),
		TokenRate: 100,
		Capacity:  100,
	})
	waiter, err := scheduler.ExecuteByConcurrency("1", buildSuccessTasks())
	this.Assert().Nil(err)
	waiter.Wait()
	this.Assert().Equal(BlockingError, waiter.ResultErr())
}

func (this *SchedulerTest) TestExecuteByOrder_ShouldReturnErrorIfBlock() {
	scheduler, _ := NewScheduler(TaskNum, nil)
	// firstly, let concurrency goruntine to occupy capacity
	waiterOne, errOne := scheduler.ExecuteByConcurrency("1", buildLongTimeTasks())
	this.Assert().Nil(errOne)
	// secondly, should return err
	waiterTwo, err := scheduler.ExecuteByOrder("2", buildSuccessTasks())
	this.Assert().Nil(err)
	waiterTwo.Wait()
	waiterOne.Wait()
	this.Assert().True(waiterOne.ResultErr() != nil || waiterTwo.ResultErr() != nil)
	fmt.Printf("one error:%+v two error:%+v \n", waiterOne.ResultErr(), waiterTwo.ResultErr())
}

func (this *SchedulerTest) Test_ShouldNotCrashIfTaskOccurPanic() {
	waiter1, err1 := this.scheduler.ExecuteByOrder("1", []*Task[TaskIface]{NewTask[TaskIface](&PanicTask{}, "", 1)})
	this.Assert().Nil(err1)
	waiter1.Wait()
	this.Assert().NotNil(waiter1.ResultErr())

	waiter2, err2 := this.scheduler.ExecuteByConcurrency("1", []*Task[TaskIface]{NewTask[TaskIface](&PanicTask{}, "", 1)})
	this.Assert().Nil(err2)
	waiter2.Wait()
	this.Assert().NotNil(waiter2.ResultErr())
}

func (this *SchedulerTest) Test_ShouldRunMuchBatchTasks() {
	scheduler, _ := NewScheduler(100000, nil)
	wg := sync.WaitGroup{}

	successTest := func(i int) {
		defer wg.Done()
		waiter, err := scheduler.ExecuteByConcurrency(strconv.Itoa(i), buildSuccessTasks())
		this.Assert().Nil(err)
		waiter.Wait()
		this.Assert().Nil(waiter.ResultErr())
	}

	failTest := func(i int) {
		defer wg.Done()
		waiter, err := scheduler.ExecuteByOrder(strconv.Itoa(i), buildFailTasks())
		this.Assert().Nil(err)
		waiter.Wait()
		this.Assert().NotNil(waiter.ResultErr())
	}

	mixTest := func(i int) {
		defer wg.Done()
		waiter, err := scheduler.ExecuteByConcurrency(strconv.Itoa(i), buildMixTasks())
		this.Assert().Nil(err)
		waiter.Wait()
		this.Assert().NotNil(waiter.ResultErr())
	}

	tests := []func(i int){
		successTest,
		failTest,
		successTest,
		mixTest,
		successTest,
	}
	for i := 1; i <= len(tests); i++ {
		wg.Add(1)
		go mixTest(i)
	}
	wg.Wait()
}

func (this *SchedulerTest) Test_ShouldBeAsync() {
	startTime := time.Now()
	waiter1, err := this.scheduler.ExecuteByOrder("1", buildSuccessTasks())
	this.Assert().Nil(err)
	waiter2, err := this.scheduler.ExecuteByConcurrency("2", buildSuccessTasks())
	this.Assert().Nil(err)
	this.Assert().Less(time.Now().Sub(startTime).Milliseconds(), int64(LongRunTime))
	waiter1.Wait()
	waiter2.Wait()
	this.Assert().Equal(len(waiter1.GetOrders()), TaskNum)
	this.Assert().Nil(waiter1.ResultErr())
	this.Assert().Equal(len(waiter2.GetOrders()), TaskNum)
	this.Assert().Nil(waiter2.ResultErr())
}

func (this *SchedulerTest) Test_ShouldCallSpecifyFunction() {
	test := func(action func(group *TaskGroup, taskId string) (bool, error)) {
		scheduler, _ := NewScheduler(100, nil)
		batchId := "1"
		_, err := scheduler.ExecuteByConcurrency(batchId, buildLongTimeTasks())
		this.Assert().Nil(err)
		ok, err := scheduler.Do(action, batchId, "1")
		this.Assert().NotNil(err)
		this.Assert().False(ok)
	}
	type action func(group *TaskGroup, taskId string) (bool, error)
	actions := []action{Stop, Resume, Cancel, Pause, Delete}
	for _, action := range actions {
		test(action)
	}
}

func (this *SchedulerTest) Test_ShouldReturnErrorIfTaskNotFind() {
	batchId := "1"
	_, err := this.scheduler.ExecuteByConcurrency(batchId, buildLongTimeTasks())
	this.Assert().Nil(err)
	ok, err := this.scheduler.Do(Stop, batchId, "10000000000000")
	this.Assert().Equal(TaskNotFindError, err)
	this.Assert().False(ok)
}

func (this *SchedulerTest) Test_ShouldReturnErrorIfTaskGroupNotExist() {
	ok, err := this.scheduler.Do(Stop, "10000000000000", "1")
	this.Assert().Equal(TaskNotFindError, err)
	this.Assert().False(ok)
}

func (this *SchedulerTest) Test_ShouldNotPanicIfCallMultiActionAtTheSameTime() {
	defer func() {
		err := recover()
		this.Assert().Nil(err)
	}()
	batchId := "1"
	_, err := this.scheduler.ExecuteByConcurrency(batchId, buildLongTimeTasks())
	this.Assert().Nil(err)
	type action func(group *TaskGroup, taskId string) (bool, error)
	var actions []action
	for i := 0; i < 20; i++ {
		actions = append(actions, []action{Stop, Resume, Cancel, Pause, Delete}...)
	}
	wg := sync.WaitGroup{}
	wg.Add(len(actions))
	for _, action := range actions {
		action := action
		go func() {
			defer wg.Done()
			this.scheduler.Do(action, batchId, "1")
		}()
	}
	wg.Wait()
}

func (this *SchedulerTest) Test_ShouldRecoverPanicFromDoFunc() {
	batchId := "1"
	_, err := this.scheduler.ExecuteByConcurrency(batchId, []*Task[TaskIface]{NewTask[TaskIface](&DoFuncPanicTask{}, batchId, 1)})
	this.Assert().Nil(err)
	_, err = this.scheduler.Do(Stop, batchId, "1")
	this.Assert().NotNil(err)
}
