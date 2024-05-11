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

type JobWrap struct {
	job         Job
	workerIndex int
}

type JobFunc func(workerId int, param map[string]any) (err error)

type Worker struct {
	jobWrapChan chan JobWrap
}

func newWorker() *Worker {
	workerObj := &Worker{
		jobWrapChan: make(chan JobWrap, 1000),
	}
	go workerObj.run()
	return workerObj
}

func (w *Worker) run() {
	for {
		select {
		case jobWrap := <-w.jobWrapChan:
			w.handle(jobWrap)
		}
	}
}

func (w *Worker) handle(jobWrap JobWrap) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("Pool执行异常: ", " jobName=", jobWrap.job.JobName, ", 参数=", jobWrap.job.JobParam, ", 异常=", err)
		}
	}()
	if jobWrap.job.WaitGroup != nil {
		defer jobWrap.job.WaitGroup.Done()
	}
	jobWrap.job.JobFunc(jobWrap.workerIndex, jobWrap.job.JobParam)
}

type Pool struct {
	jobChan     chan Job
	workerSize  int
	workers     []*Worker
	workerIndex int
	lock        sync.Mutex
}

var poolObj *Pool
var poolOnce sync.Once

func NewPool(queueSize, workerSize int) *Pool {
	poolOnce.Do(func() {
		poolObj = &Pool{
			jobChan:     make(chan Job, queueSize),
			workerSize:  workerSize,
			workers:     make([]*Worker, workerSize),
			workerIndex: 0,
		}
		for i := 0; i < workerSize; i++ {
			// #issue: worker不能才有once
			poolObj.workers[i] = newWorker()
		}
		go poolObj.run()
	})
	return poolObj
}

func (p *Pool) ExecTask(job Job) {
	p.jobChan <- job
}

func (p *Pool) run() {
	for {
		select {
		case job := <-p.jobChan:
			index := p.getIndex()
			p.workers[index].jobWrapChan <- JobWrap{job: job, workerIndex: index}
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
