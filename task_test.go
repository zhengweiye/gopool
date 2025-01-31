package gopool

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

func Test_Task(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	workerPool := NewPool(100, 20, 100, ctx, wg)

	for i := 0; i < 5; i++ {
		workerPool.ExecTask(Job{
			JobName: fmt.Sprintf("作业名称%d", i),
			JobFunc: myTask,
			JobParam: map[string]any{
				"name":    fmt.Sprintf("姓名%d", i),
				"address": fmt.Sprintf("地址%d", i),
			},
		})
	}

	time.Sleep(5 * time.Second)
	cancel()
	//pool.Shutdown()

	wg.Wait()
	fmt.Println("结束.....")
}

func myTask(workerId int, jobName string, param map[string]any) (err error) {
	//fmt.Println("before: jobName=", "线程Id=", workerId, jobName, ", 参数=", param)
	rand.Seed(time.Now().UnixNano())
	t := rand.Intn(10)
	time.Sleep(time.Duration(int64(time.Second) * int64(t)))
	fmt.Println("after: jobName=", "线程Id=", workerId, jobName, ", 参数=", param)
	return
}
