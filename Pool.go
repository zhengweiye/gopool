package gopool

import (
	"context"
	"fmt"
	"sync"
)

type Job struct {
	JobName  string
	JobFunc  JobFunc
	JobParam map[string]any
}

type JobFuture struct {
	JobName  string
	JobFunc  JobFuncFuture
	JobParam map[string]any
	Future   chan Future
}

type Future struct {
	Error  error
	Result any
}

type JobWrap struct {
	job         Job
	workerIndex int
	wg          *sync.WaitGroup
}

type JobFutureWrap struct {
	job         JobFuture
	workerIndex int
	wg          *sync.WaitGroup
}

type JobFunc func(workerId int, jobName string, param map[string]any) (err error)
type JobFuncFuture func(workerId int, jobName string, param map[string]any, future chan Future)

type Worker struct {
	index             int
	jobWrapChan       chan JobWrap
	jobFutureWrapChan chan JobFutureWrap
	quitChan          chan bool
}

func newWorker(index int) *Worker {
	workerObj := &Worker{
		index:             index,
		jobWrapChan:       make(chan JobWrap, 1000),
		jobFutureWrapChan: make(chan JobFutureWrap, 1000),
		quitChan:          make(chan bool, 1),
	}

	go workerObj.run()

	return workerObj
}

func (w *Worker) shutdown() {
	// 退出监听
	//close(w.jobWrapChan)
	//close(w.jobFutureWrapChan)
	close(w.quitChan)
}

func (w *Worker) run() {
	for {
		select {
		case jobWrap := <-w.jobWrapChan:
			w.handle(jobWrap)

		case jobFutureWrap := <-w.jobFutureWrapChan:
			w.handleFuture(jobFutureWrap)

		case <-w.quitChan:
			//fmt.Printf("worker[%d] quitChan select is quit.\n", w.index)
			return
		}
	}
}

func (w *Worker) handle(jobWrap JobWrap) {
	jobWrap.wg.Add(1)
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("Pool.handle()执行异常: ", " jobName=", jobWrap.job.JobName, ", 参数=", jobWrap.job.JobParam, ", 异常=", err)
		}
		jobWrap.wg.Done()
	}()

	if jobWrap.job.JobFunc != nil {
		jobWrap.job.JobFunc(jobWrap.workerIndex, jobWrap.job.JobName, jobWrap.job.JobParam)
	} else {
		//fmt.Println(">>>>[handle], workIndex=", w.index, ", 任务名称=", jobWrap.job.JobName, ", 的业务函数为空")
	}
}

func (w *Worker) handleFuture(jobFutureWrap JobFutureWrap) {
	jobFutureWrap.wg.Add(1)
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("Pool.handleFuture()执行异常: ", " jobName=", jobFutureWrap.job.JobName, ", 参数=", jobFutureWrap.job.JobParam, ", 异常=", err)
		}
		jobFutureWrap.wg.Done()
	}()

	// 关闭
	if jobFutureWrap.job.Future != nil {
		defer close(jobFutureWrap.job.Future)
	}

	// 执行
	if jobFutureWrap.job.JobFunc != nil {
		jobFutureWrap.job.JobFunc(jobFutureWrap.workerIndex, jobFutureWrap.job.JobName, jobFutureWrap.job.JobParam, jobFutureWrap.job.Future)
	} else {
		//fmt.Println(">>>>[handleFuture], workIndex=", w.index, ", 任务名称=", jobFutureWrap.job.JobName, ", 的业务函数为空")
	}
}

type Pool struct {
	jobChan       chan Job
	jobFutureChan chan JobFuture
	workerSize    int
	workers       []*Worker
	workerIndex   int
	quitChan      chan bool
	lock          sync.Mutex
	ctx           context.Context
	isShutdown    bool
	waitGroup     sync.WaitGroup
}

var poolObj *Pool
var poolOnce sync.Once

func NewPool(queueSize, workerSize int, ctx context.Context) *Pool {
	poolOnce.Do(func() {
		if ctx == nil {
			ctx = context.Background()
		}
		wg := sync.WaitGroup{}
		poolObj = &Pool{
			jobChan:       make(chan Job, queueSize),
			jobFutureChan: make(chan JobFuture, queueSize),
			workerSize:    workerSize,
			workers:       make([]*Worker, workerSize),
			workerIndex:   0,
			quitChan:      make(chan bool, 1),
			ctx:           ctx,
			isShutdown:    false,
			waitGroup:     wg,
		}

		for i := 0; i < workerSize; i++ {
			// #issue: worker不能才有once
			poolObj.workers[i] = newWorker(i)
		}

		go poolObj.run()
	})
	return poolObj
}

func (p *Pool) Shutdown() {
	//TODO 测试优雅停止、ws、pool是否共用测试、工作流优化

	// 停止接受任务
	p.isShutdown = true

	// 等待任务执行完成
	fmt.Println(">>>>>>>>>>>>>>>>>>>>>>>>>>通知Worker关闭, 进行中....<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
	p.waitGroup.Wait()

	// 关闭Worker的chan
	for _, worker := range p.workers {
		worker.shutdown()
	}

	// 退出监听
	//close(p.jobChan)
	//close(p.jobFutureChan)
	close(p.quitChan)
	fmt.Println(">>>>>>>>>>>>>>>>>>>>>>>>>>通知Worker关闭, 已结束....<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
}

func (p *Pool) ExecTask(job Job) {
	if len(job.JobName) == 0 {
		panic(fmt.Errorf("JobName为空"))
	}
	if job.JobFunc == nil {
		panic(fmt.Errorf("JobFunc为空"))
	}
	if !p.isShutdown {
		p.jobChan <- job
	} else {
		fmt.Println(">>>[无返回值]协程池已经关闭,无法再接受新的任务....")
	}
}

func (p *Pool) ExecTaskFuture(job JobFuture) {
	if len(job.JobName) == 0 {
		panic(fmt.Errorf("JobName为空"))
	}
	if job.JobFunc == nil {
		panic(fmt.Errorf("JobFunc为空"))
	}
	if job.Future == nil {
		panic(fmt.Errorf("Future为空"))
	}
	if !p.isShutdown {
		p.jobFutureChan <- job
	} else {
		fmt.Println(">>>[有返回值]协程池已经关闭,无法再接受新的任务....")
	}
}

func (p *Pool) run() {
	for {
		select {
		case job := <-p.jobChan:
			index := p.getIndex()

			c := p.workers[index].jobWrapChan
			if c != nil {
				c <- JobWrap{job: job, workerIndex: index, wg: &p.waitGroup}
			}

		case jobFuture := <-p.jobFutureChan:
			index := p.getIndex()
			c := p.workers[index].jobFutureWrapChan
			if c != nil {
				c <- JobFutureWrap{job: jobFuture, workerIndex: index, wg: &p.waitGroup}
			}

		case <-p.quitChan:
			fmt.Println(">>>pool quitChan select is quit.")
			return

		case <-p.ctx.Done():
			fmt.Println(">>>pool context Done.")
			if !p.isShutdown {
				p.Shutdown()
			}
		}
	}
}

func (p *Pool) getIndex() int {
	p.lock.Lock()
	defer p.lock.Unlock()
	p.workerIndex += 1
	if p.workerIndex >= p.workerSize {
		p.workerIndex = 0
	}
	return p.workerIndex
}
