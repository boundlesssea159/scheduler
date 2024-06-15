[![codecov](https://codecov.io/github/boundlesssea159/scheduler/graph/badge.svg?token=186QDXOZV5)](https://codecov.io/github/boundlesssea159/scheduler)
# Scheduler-Task Manager
Scheduler, the sub-task manager, provide the ability to manage the execution sequence of sub-task which are split from a main task and control the whole concurrency  
## Features
* Provide different ways to execute sub-task
* Control the whole concurrency by user self
## Usages
```go
// first parameter: means the maximum concurrency
// second parameter: the configuration of Limiter 
scheduler, err := NewScheduler(100, &LimiterParams{
		Ctx:       context.Background(),
		TokenRate: 1, // the token given rate
		Capacity:  50,// the maximum concurrency allowed by Limiter in some special scene
	})
if err! = nil {
	return
}

// CASE 1 : execute by order
// "1" means the main task id
waiter1, err := scheduler.ExecuteByOrder("1", []*Task[TaskIface]{
	// as the sub-tasks in there
})
if err!=nil{
	return
}
waiter1.Wait() // wait done
fmt.Println(waiter1.GetOrders()) // print the finishing order of task 

// CASE 2 : execute by concurrency
waiter2, err := scheduler.ExecuteByConcurrency("2", []*Task[TaskIface]{
})
if err!=nil{
    return
}
waiter2.Wait()
fmt.Println(waiter2.GetOrders())
```