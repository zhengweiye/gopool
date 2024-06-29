package main

import (
	"context"
	"fmt"
	"github.com/zhengweiye/gopool"
	"sync"
	"time"
)

/**
 * 线程池执行
 */
func main() {
	ctx, cancel := context.WithCancel(context.Background())
	pool := gopool.NewPool(10, 5, ctx)
	//wg := sync.WaitGroup{}
	for i := 0; i < 5; i++ {
		//.Add(1)
		//defer wg.Done()
		pool.ExecTask(gopool.Job{
			JobName: fmt.Sprintf("作业名称%d", i),
			JobFunc: myJob,
			JobParam: map[string]any{
				"name":    fmt.Sprintf("姓名%d", i),
				"address": fmt.Sprintf("地址%d", i),
			},
		})
	}
	//wg.Wait()

	time.Sleep(1 * time.Second)
	cancel()
	//pool.Shutdown()

	for {

	}
}

func myJob(workerId int, jobName string, param map[string]any) (err error) {
	//fmt.Println("before: jobName=", "线程Id=", workerId, jobName, ", 参数=", param)
	time.Sleep(1 * time.Second)
	fmt.Println("after: jobName=", "线程Id=", workerId, jobName, ", 参数=", param)
	return
}

func main2() {
	beginTime := time.Now()
	pool := gopool.NewPool(1000, 1000, nil)
	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		futureChan := make(chan gopool.Future)
		pool.ExecTaskFuture(gopool.JobFuture{
			JobName: fmt.Sprintf("作业名称%d", i),
			JobFunc: nil, //myJobFuture,
			JobParam: map[string]any{
				"num": i,
			},
			Future: futureChan,
		})
		wg.Done()
		future := <-futureChan
		fmt.Println("i=", i, "异常: ", future.Error, ", 结果=", future.Result)
	}
	wg.Wait()
	endTime := time.Now()
	fmt.Println("执行完成,共耗时=", endTime.Sub(beginTime).Seconds(), "秒")
}

func myJobFuture(workerId int, jobName string, param map[string]any, future chan gopool.Future) {
	var result int
	defer func() {
		if err := recover(); err != nil {
			future <- gopool.Future{
				Error: fmt.Errorf("%v", err),
			}
		} else {
			future <- gopool.Future{
				Result: result,
			}
		}
		// 在Pool里面关闭
		//close(future)
	}()

	result = 2 * param["num"].(int)
	//result = 100 / param["num"].(int)
}
