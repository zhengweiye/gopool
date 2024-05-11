package main

import (
	"fmt"
	"github.com/zhengweiye/gopool"
	"sync"
	"time"
)

/**
 * 非线程池执行
 */
func main1() {
	beginTime := time.Now()
	wg := sync.WaitGroup{}
	for i := 0; i < 100000; i++ {
		wg.Add(1)
		myJob(i, map[string]any{
			"name":    fmt.Sprintf("姓名%d", i),
			"address": fmt.Sprintf("地址%d", i),
		})
		wg.Done()
	}
	wg.Wait()
	endTime := time.Now()
	fmt.Println("执行完成,共耗时=", endTime.Sub(beginTime).Seconds(), "秒")
}

/**
 * 线程池执行
 */
func main2() {
	beginTime := time.Now()
	pool := gopool.NewPool(1000, 1000)
	wg := sync.WaitGroup{}
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		pool.ExecTask(gopool.Job{
			WaitGroup: &wg,
			JobName:   fmt.Sprintf("作业名称%d", i),
			JobFunc:   myJob,
			JobParam: map[string]any{
				"name":    fmt.Sprintf("姓名%d", i),
				"address": fmt.Sprintf("地址%d", i),
			},
		})
	}
	wg.Wait()
	endTime := time.Now()
	fmt.Println("执行完成,共耗时=", endTime.Sub(beginTime).Seconds(), "秒")
}

func myJob(workerId int, param map[string]any) (err error) {
	time.Sleep(1 * time.Second)
	fmt.Println("执行成功: ", "线程", workerId, param)
	return nil
}

func main() {
	beginTime := time.Now()
	pool := gopool.NewPool(1000, 1000)
	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		futureChan := make(chan gopool.Future)
		pool.ExecTaskFuture(gopool.JobFuture{
			WaitGroup: &wg,
			JobName:   fmt.Sprintf("作业名称%d", i),
			JobFunc:   myJobFuture,
			JobParam: map[string]any{
				"num": i,
			},
			Future: futureChan,
		})
		future := <-futureChan
		fmt.Println("i=", i, "异常: ", future.Error, ", 结果=", future.Result)
	}
	wg.Wait()
	endTime := time.Now()
	fmt.Println("执行完成,共耗时=", endTime.Sub(beginTime).Seconds(), "秒")
}

func myJobFuture(workerId int, param map[string]any, future chan gopool.Future) {
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
	}()

	//result = 2*param["num"].(int)
	result = 100 / param["num"].(int)
}
