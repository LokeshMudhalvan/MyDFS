package workers

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var uniqueId atomic.Uint32

type WorkerPool interface {
	Submit(Job) error
	Shutdown()
}

type JobStatus int

const (
	StatusSuccess JobStatus = iota
	StatusFailed
	StatusCancelled
)

type jobFunc func(ctx context.Context) (interface{}, error)

type Job struct {
	id   int
	work jobFunc
	// results is the channel the channel where the job result will be sent
	results chan<- Result
}

func NewJob(work jobFunc, results chan<- Result) Job {
	id := uniqueId.Add(1)
	return Job{
		id:      int(id),
		work:    work,
		results: results,
	}
}

type RetriablePoolOptions func(*poolConfig)

type Result struct {
	Output   interface{}
	Error    error
	JobId    int
	Retries  int
	Duration time.Duration
	Status   JobStatus
}

type poolConfig struct {
	minWorkers      int
	maxWorkers      int
	maxRetries      int
	retryDelay      time.Duration
	bufferSize      int
	shutdownTimeout time.Duration
}

func WithMinWorkers(workers int) RetriablePoolOptions {
	return func(p *poolConfig) {
		p.minWorkers = workers
	}
}

func WithMaxWorkers(workers int) RetriablePoolOptions {
	return func(p *poolConfig) {
		p.maxWorkers = workers
	}
}

func WithMaxRetries(retries int) RetriablePoolOptions {
	return func(p *poolConfig) {
		p.maxRetries = retries
	}
}

func WithRetryDelay(delay time.Duration) RetriablePoolOptions {
	return func(p *poolConfig) {
		p.retryDelay = delay
	}
}

func WithBufferSize(bufferSize int) RetriablePoolOptions {
	return func(p *poolConfig) {
		p.bufferSize = bufferSize
	}
}

func WithShutdownTimeout(timeout time.Duration) RetriablePoolOptions {
	return func(p *poolConfig) {
		p.shutdownTimeout = timeout
	}
}

func defaultPoolConfig() *poolConfig {
	return &poolConfig{
		minWorkers:      3,
		maxWorkers:      6,
		bufferSize:      6,
		maxRetries:      3,
		retryDelay:      2 * time.Second,
		shutdownTimeout: 7 * time.Second,
	}
}

type RetriableWorkerPool struct {
	poolConfig    *poolConfig
	totalWorkers  int
	activeWorkers int
	jobs          chan Job
	mu            sync.Mutex
	wg            sync.WaitGroup
	ctx           context.Context
	cancel        context.CancelFunc
	close         chan struct{}
	submitMu      sync.RWMutex
	isClosed      bool
}

func NewRetriableWorkerPool(ctx context.Context, opts ...RetriablePoolOptions) *RetriableWorkerPool {
	ctx, cancel := context.WithCancel(ctx)
	conf := defaultPoolConfig()

	for _, opt := range opts {
		opt(conf)
	}

	pool := &RetriableWorkerPool{
		poolConfig: conf,
		ctx:        ctx,
		cancel:     cancel,
		jobs:       make(chan Job, conf.bufferSize),
		close:      make(chan struct{}),
	}
	pool.start()
	go pool.scaleWorkers()

	return pool
}

func (w *RetriableWorkerPool) Submit(job Job) error {
	w.submitMu.RLock()
	defer w.submitMu.RUnlock()

	if w.isClosed {
		return fmt.Errorf("failed to add job: Worker pool closed already")
	}
	select {
	case <-w.ctx.Done():
		return fmt.Errorf("failed to add job: Worker pool closed already")
	case w.jobs <- job:
		return nil
	}
}

func (w *RetriableWorkerPool) Shutdown() {
	w.submitMu.Lock()
	if w.isClosed {
		fmt.Println("Worker pool shutdown skipped. Worker pool closed already")
		return
	}
	close(w.jobs)
	w.isClosed = true
	w.submitMu.Unlock()
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()
	select {
	case <-time.After(w.poolConfig.shutdownTimeout):
		w.cancel()
		fmt.Println("worker pool shutdown timed out")
	case <-done:
		w.cancel()
		fmt.Println("worker pool shutdown")
	}
}

func (w *RetriableWorkerPool) start() {
	for i := 1; i <= w.poolConfig.minWorkers; i++ {
		w.addWorker()
	}
}

func (w *RetriableWorkerPool) addWorker() {
	w.wg.Add(1)
	go w.worker()
	w.mu.Lock()
	w.totalWorkers++
	w.mu.Unlock()
}

func (w *RetriableWorkerPool) worker() {
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
			case job.results <- result:
			}
		case <-w.close:
			return
		}
	}
}

func (w *RetriableWorkerPool) performWork(job Job) Result {
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
		res, err := job.work(w.ctx)
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

func (w *RetriableWorkerPool) isRetriable(err error) bool {
	// TODO: Add specific error types to fail and not retry for
	return true
}

func (w *RetriableWorkerPool) scaleWorkers() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

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
				select {
				case w.close <- struct{}{}:
					fmt.Println("scaling down the worker")
				case <-w.ctx.Done():
					return
				}
			}
		}
	}
}
