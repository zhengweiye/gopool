package gopool

import (
	"fmt"
	"sync"
)

type Job struct {
	JobName   string
	JobFunc   JobFunc
	WaitGroup *sync.WaitGroup
	JobParam  map[string]any
}

type JobFuture struct {
	JobName   string
	JobFunc   JobFuncFuture
	WaitGroup *sync.WaitGroup
	JobParam  map[string]any
	Future    chan Future
}

type Future struct {
	Error  error
	Result any
}

type JobWrap struct {
	job         Job
	workerIndex int
}

type JobFutureWrap struct {
	job         JobFuture
	workerIndex int
}

type JobFunc func(workerId int, param map[string]any) (err error)
type JobFuncFuture func(workerId int, param map[string]any, future chan Future)

type Worker struct {
	jobWrapChan       chan JobWrap
	jobFutureWrapChan chan JobFutureWrap
	quitChan          chan bool
}

func newWorker() *Worker {
	workerObj := &Worker{
		jobWrapChan:       make(chan JobWrap, 1000),
		jobFutureWrapChan: make(chan JobFutureWrap, 1000),
		quitChan:          make(chan bool, 1),
	}
	go workerObj.run()
	return workerObj
}

func (w *Worker) run() {
	for {
		select {
		case jobWrap := <-w.jobWrapChan:
			w.handle(jobWrap)

		case jobFutureWrap := <-w.jobFutureWrapChan:
			w.handleFuture(jobFutureWrap)
		case <-w.quitChan:
			fmt.Println("worker quitChan select is quit.")
			return
		}
	}
}

func (w *Worker) handle(jobWrap JobWrap) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("Pool.handle()执行异常: ", " jobName=", jobWrap.job.JobName, ", 参数=", jobWrap.job.JobParam, ", 异常=", err)
		}
	}()
	if jobWrap.job.WaitGroup != nil {
		defer jobWrap.job.WaitGroup.Done()
	}
	jobWrap.job.JobFunc(jobWrap.workerIndex, jobWrap.job.JobParam)
}

func (w *Worker) handleFuture(jobFutureWrap JobFutureWrap) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("Pool.handleFuture()执行异常: ", " jobName=", jobFutureWrap.job.JobName, ", 参数=", jobFutureWrap.job.JobParam, ", 异常=", err)
		}
	}()
	if jobFutureWrap.job.WaitGroup != nil {
		defer jobFutureWrap.job.WaitGroup.Done()
	}
	// 关闭
	defer close(jobFutureWrap.job.Future)
	// 执行
	jobFutureWrap.job.JobFunc(jobFutureWrap.workerIndex, jobFutureWrap.job.JobParam, jobFutureWrap.job.Future)

}

type Pool struct {
	jobChan       chan Job
	jobFutureChan chan JobFuture
	workerSize    int
	workers       []*Worker
	workerIndex   int
	quitChan      chan bool
	lock          sync.Mutex
}

var poolObj *Pool
var poolOnce sync.Once

func NewPool(queueSize, workerSize int) *Pool {
	poolOnce.Do(func() {
		poolObj = &Pool{
			jobChan:       make(chan Job, queueSize),
			jobFutureChan: make(chan JobFuture, queueSize),
			workerSize:    workerSize,
			workers:       make([]*Worker, workerSize),
			workerIndex:   0,
			quitChan:      make(chan bool, 1),
		}
		for i := 0; i < workerSize; i++ {
			// #issue: worker不能才有once
			poolObj.workers[i] = newWorker()
		}
		go poolObj.run()
	})
	return poolObj
}

func (p *Pool) Shutdown() {
	for _, worker := range p.workers {
		close(worker.quitChan)
		close(worker.jobWrapChan)
		close(worker.jobFutureWrapChan)
	}
	close(p.quitChan)
	close(p.jobChan)
	close(p.jobFutureChan)
}

func (p *Pool) ExecTask(job Job) {
	if len(job.JobName) == 0 {
		panic(fmt.Errorf("JobName为空"))
	}
	if job.JobFunc == nil {
		panic(fmt.Errorf("JobFunc为空"))
	}
	p.jobChan <- job
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
	p.jobFutureChan <- job
}

func (p *Pool) run() {
	for {
		select {
		case job := <-p.jobChan:
			index := p.getIndex()
			p.workers[index].jobWrapChan <- JobWrap{job: job, workerIndex: index}
		case jobFuture := <-p.jobFutureChan:
			index := p.getIndex()
			p.workers[index].jobFutureWrapChan <- JobFutureWrap{job: jobFuture, workerIndex: index}
		case <-p.quitChan:
			fmt.Println("pool quitChan select is quit.")
			return
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
