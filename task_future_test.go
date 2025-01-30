package gopool

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func Test_Task_Future(t *testing.T) {
	ctx, _ := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	workerPool := NewPool(100, 20, 100, ctx, wg)
	var futureList []chan Future
	for i := 0; i < 5; i++ {
		future := make(chan Future)
		workerPool.ExecTaskFuture(JobFuture{
			JobName: "测试",
			JobFunc: deal,
			JobParam: map[string]any{
				"id": i,
			},
			Future: future,
		})
		futureList = append(futureList, future)
		//TODO 串行执行
		/*result := <-future
		if result.Error != nil {
			panic(result.Error)
		}
		fmt.Printf("执行：id=%d, result=%v\n", i, result.Result)*/
	}

	//TODO 并行执行
	var resultList []Future
	for _, future := range futureList {
		result := <-future
		resultList = append(resultList, result)
		//TODO 内部已经关闭了
		//close(future)
	}
	for _, result := range resultList {
		if result.Error != nil {
			panic(result.Error)
		}
		fmt.Printf("result=%v\n", result.Result)
	}
}

func deal(workerId int, jobName string, param map[string]any, future chan Future) {
	var name string
	defer func() {
		if err := recover(); err != nil {
			future <- Future{
				Error: fmt.Errorf("%v", err),
			}
		} else {
			future <- Future{
				Result: name,
			}
		}
	}()

	id := param["id"].(int)
	/*	if id == 3 {
		panic("测试异常")
	}*/
	fmt.Println("id=", id, ", 开始执行")
	time.Sleep(10 * time.Second)
	fmt.Println("id=", id, ", 结束执行")
	name = fmt.Sprintf("%d", id+10)
}
