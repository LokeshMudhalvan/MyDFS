package workers

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// TODO: Add a way to generate a truly unique id
var (
	uniqueId atomic.Uint32
)

type JobStatus int

const (
	StatusSuccess JobStatus = iota
	StatusFailed
	StatusCancelled
)

type jobFunc func() (interface{}, error)

type Job struct {
	id   int
	work jobFunc
}

func NewJob(work jobFunc) Job {
	id := uniqueId.Add(1)
	return Job{
		id:   int(id),
		work: work,
	}
}

type Result struct {
	Output   interface{}
	Error    error
	JobId    int
	Retries  int
	Duration time.Duration
	Status   JobStatus
}

type poolConfig struct {
	minWorkers int
	maxWorkers int
	maxRetries int
	retryDelay time.Duration
}

func NewPoolConfig(minWorkers int, maxWorkers int, maxRetries int, retryDelay time.Duration) poolConfig {
	return poolConfig{
		minWorkers: minWorkers,
		maxWorkers: maxWorkers,
		maxRetries: maxRetries,
		retryDelay: retryDelay,
	}
}

type WorkerPool struct {
	poolConfig    poolConfig
	totalWorkers  int
	activeWorkers int
	jobs          chan Job
	results       chan Result
	mu            sync.Mutex
	wg            sync.WaitGroup
	ctx           context.Context
	cancel        context.CancelFunc
	close         chan struct{}
}

func NewWorkerPool(poolConfig poolConfig, bufferSize int) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())
	pool := &WorkerPool{
		poolConfig: poolConfig,
		ctx:        ctx,
		cancel:     cancel,
		jobs:       make(chan Job, bufferSize),
		results:    make(chan Result, bufferSize),
		close:      make(chan struct{}),
	}
	pool.start()
	go pool.scaleWorkers()

	return pool
}

func (w *WorkerPool) Submit(job Job) error {
	select {
	case <-w.ctx.Done():
		return fmt.Errorf("failed to add job: Worker pool closed already")
	case w.jobs <- job:
		return nil
	}
}

func (w *WorkerPool) Results() <-chan Result {
	return w.results
}

func (w *WorkerPool) Shutdown() {
	close(w.jobs)

	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()
	select {
	case <-time.After(2 * time.Minute):
		w.cancel()
		fmt.Println("worker pool shutdown timed out")
	case <-done:
		w.cancel()
		fmt.Println("worker pool shutdown")
	}
	close(w.results)
}

func (w *WorkerPool) start() {
	for i := 1; i <= w.poolConfig.minWorkers; i++ {
		w.addWorker()
	}
}

func (w *WorkerPool) addWorker() {
	w.wg.Add(1)
	go w.worker()
	w.mu.Lock()
	w.totalWorkers++
	w.mu.Unlock()
}

func (w *WorkerPool) worker() {
	defer w.wg.Done()
	defer func() {
		w.mu.Lock()
		w.totalWorkers--
		w.mu.Unlock()
	}()

	for {
		select {
		case <-w.ctx.Done():
			return
		case job, ok := <-w.jobs:
			if !ok {
				return
			}
			w.mu.Lock()
			w.activeWorkers++
			w.mu.Unlock()
			result := w.performWork(job)
			w.mu.Lock()
			w.activeWorkers--
			w.mu.Unlock()

			select {
			case <-w.ctx.Done():
				return
			case w.results <- result:
			}
		case <-w.close:
			return
		}
	}
}

func (w *WorkerPool) performWork(job Job) Result {
	retriable := true
	retries := -1
	var lastErr error
	startTime := time.Now()

	for i := 0; i <= w.poolConfig.maxRetries; i++ {
		if !retriable {
			break
		}
		retries++
		wait := w.poolConfig.retryDelay * time.Duration(1<<retries)
		if retries == 0 {
			wait = 0
		}
		select {
		case <-time.After(wait):
		case <-w.ctx.Done():
			return Result{
				JobId:    job.id,
				Status:   StatusCancelled,
				Retries:  retries,
				Duration: time.Since(startTime),
			}
		}
		res, err := job.work()
		if err != nil {
			lastErr = err
			retriable = w.isRetriable(err)
			continue
		}

		return Result{
			Output:   res,
			JobId:    job.id,
			Status:   StatusSuccess,
			Retries:  retries,
			Duration: time.Since(startTime),
		}
	}

	return Result{
		Error:    lastErr,
		JobId:    job.id,
		Status:   StatusFailed,
		Retries:  retries,
		Duration: time.Since(startTime),
	}
}

func (w *WorkerPool) isRetriable(err error) bool {
	// TODO: Add specific error types to fail and not retry for
	return true
}

func (w *WorkerPool) scaleWorkers() {
	ticker := time.NewTicker(1 * time.Second)

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			jobs := len(w.jobs)
			w.mu.Lock()
			workers := w.totalWorkers
			active := w.activeWorkers
			w.mu.Unlock()
			if jobs > workers && workers == active && workers < w.poolConfig.maxWorkers {
				w.addWorker()
			}

			if jobs == 0 && active < workers/2 && workers > w.poolConfig.minWorkers {
				w.close <- struct{}{}
				fmt.Println("scaling down the worker")
			}
		}
	}
}
