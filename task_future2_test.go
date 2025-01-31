package gopool

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func Test_Task_Future2(t *testing.T) {
	ctx, _ := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	workerPool := NewPool(100, 20, 100, ctx, wg)
	var futureList []chan Future
	globalParam := "zwy"
	for i := 0; i < 3; i++ {
		future := make(chan Future)
		workerPool.ExecTaskFuture2(JobFuture2{
			JobName: "测试",
			JobFunc: myTaskFuture2,
			JobParam: map[string]any{
				"id": i,
			},
			Future: future,
		}, &globalParam)
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
	fmt.Println("globalParam=", globalParam)
}

func myTaskFuture2(workerId int, jobName string, param map[string]any, future chan Future, globalParam any) {
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

	// 修改全局变量的值
	globalValue := globalParam.(*string)
	*globalValue = "yf"
}
