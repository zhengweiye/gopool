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

func newWorker(index, queueSize int) *Worker {
	workerObj := &Worker{
		index:             index,
		jobWrapChan:       make(chan JobWrap, queueSize),
		jobFutureWrapChan: make(chan JobFutureWrap, queueSize),
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
	jobWrap.wg.Add(1) //TODO 这里加1
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf(">>> [协程池] 名称[%s] 参数[%v] 异常：[%v] \n", jobWrap.job.JobName, jobWrap.job.JobParam, err)
		}
		jobWrap.wg.Done() //TODO 这里Done()
	}()

	if jobWrap.job.JobFunc != nil {
		jobWrap.job.JobFunc(jobWrap.workerIndex, jobWrap.job.JobName, jobWrap.job.JobParam)
	} else {
		//fmt.Println(">>>>[handle], workIndex=", w.index, ", 任务名称=", jobWrap.job.JobName, ", 的业务函数为空")
	}
}

func (w *Worker) handleFuture(jobFutureWrap JobFutureWrap) {
	jobFutureWrap.wg.Add(1) //TODO 这里加1
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf(">>> [协程池] 名称[%s] 参数[%v] 异常：[%v] \n", jobFutureWrap.job.JobName, jobFutureWrap.job.JobParam, err)
		}
		jobFutureWrap.wg.Done() //TODO 这里Done()
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
	jobChan         chan Job
	jobFutureChan   chan JobFuture
	workerSize      int
	workers         []*Worker
	workerIndex     int
	quitChan        chan bool
	lock            sync.Mutex
	ctx             context.Context
	isShutdown      bool
	innerWaitGroup  *sync.WaitGroup
	globalWaitGroup *sync.WaitGroup
}

var poolObj *Pool
var poolOnce sync.Once

/**
 * poolQueueSize 协程池的队列大小
 * workerSize 协程池池有几个工作者（执行业务处理）
 * workerQueueSize 每个工作者的队列大小
 */

func NewPool(poolQueueSize, workerSize, workerQueueSize int, ctx context.Context, waitGroup *sync.WaitGroup) *Pool {
	poolOnce.Do(func() {
		waitGroup.Add(1) //TODO 必须加1，在Shutdown时Done()
		innerWaitGroup := &sync.WaitGroup{}
		poolObj = &Pool{
			jobChan:         make(chan Job, poolQueueSize),
			jobFutureChan:   make(chan JobFuture, poolQueueSize),
			workerSize:      workerSize,
			workers:         make([]*Worker, workerSize),
			workerIndex:     0,
			quitChan:        make(chan bool, 1),
			ctx:             ctx,
			isShutdown:      false,
			innerWaitGroup:  innerWaitGroup,
			globalWaitGroup: waitGroup,
		}

		for i := 0; i < workerSize; i++ {
			// #issue: worker不能才有once
			poolObj.workers[i] = newWorker(i, workerQueueSize)
		}

		go poolObj.run()
	})
	return poolObj
}

func (p *Pool) Shutdown() {
	// 停止接受任务
	p.isShutdown = true

	// 等待任务执行完成
	fmt.Println(">>>>>>>>>>>>>>>>>>>>>>>>>>[协程池] 通知Worker关闭, 进行中....<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
	p.innerWaitGroup.Wait()

	// 关闭Worker的chan
	for _, worker := range p.workers {
		worker.shutdown()
	}

	// 退出监听
	//close(p.jobChan)
	//close(p.jobFutureChan)
	close(p.quitChan)

	// 通知业务系统的http服务监听
	p.globalWaitGroup.Done()

	fmt.Println(">>>>>>>>>>>>>>>>>>>>>>>>>>[协程池] 通知Worker关闭, 已结束....<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
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
		fmt.Println(">>>[协程池] 协程池已经关闭,无法再接受新的任务....")
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
		fmt.Println(">>>[协程池] 协程池已经关闭,无法再接受新的任务....")
	}
}

func (p *Pool) run() {
	for {
		select {
		case job := <-p.jobChan:
			index := p.getIndex()

			c := p.workers[index].jobWrapChan
			if c != nil {
				c <- JobWrap{job: job, workerIndex: index, wg: p.innerWaitGroup}
			}

		case jobFuture := <-p.jobFutureChan:
			index := p.getIndex()
			c := p.workers[index].jobFutureWrapChan
			if c != nil {
				c <- JobFutureWrap{job: jobFuture, workerIndex: index, wg: p.innerWaitGroup}
			}

		case <-p.quitChan:
			fmt.Println(">>> [协程池] 退出协程池监听...")
			return

		case <-p.ctx.Done():
			fmt.Println(">>> [协程池] 监听到Context取消信号...")
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
