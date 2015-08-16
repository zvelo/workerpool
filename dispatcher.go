package workerpool

import "golang.org/x/net/context"

type job struct {
	ctx  context.Context
	data interface{}
}

// The Handler interface is used by workers to execute a job
type Handler interface {
	Do(context.Context, interface{})
}

// The HandlerFunc type is an adapter to allow the use of ordinary functions as
// worker handlers
type HandlerFunc func(context.Context, interface{})

// Do calls f(ctx, data)
func (f HandlerFunc) Do(ctx context.Context, data interface{}) {
	f(ctx, data)
}

// A Dispatcher maintains a pool of workers and sends them jobs
type Dispatcher struct {
	// A buffered channel that we can send work requests on.
	jobQueue    chan job
	jobChannels chan chan job
	maxWorkers  int
	handler     Handler
	workers     []worker
	quit        chan bool
}

// NewDispatcher allocates a new Dispatcher
func NewDispatcher(maxQueue, maxWorkers int, handler Handler) Dispatcher {
	if maxWorkers == 0 {
		maxWorkers = 1
	}

	ret := Dispatcher{
		jobQueue:    make(chan job, maxQueue),
		jobChannels: make(chan chan job, maxWorkers),
		handler:     handler,
		workers:     make([]worker, maxWorkers),
	}

	for i := range ret.workers {
		ret.workers[i] = newWorker(ret, i)
	}

	return ret
}

// Start the dispatcher and all workers
func (d Dispatcher) Start() Dispatcher {
	for _, w := range d.workers {
		w.Start()
	}

	go func() {
		for {
			select {
			case j := <-d.jobQueue:
				// a job request has been received
				go func() {
					// try to obtain a worker job channel that is available.
					// this will block until a worker is idle
					jobCh := <-d.jobChannels

					// dispatch the job to the worker job channel
					jobCh <- j
				}()
			case <-d.quit:
				for _, w := range d.workers {
					w.Stop()
				}
				return
			}
		}
	}()

	return d
}

// Stop the Dispatcher and all workers
func (d Dispatcher) Stop() {
	go func() {
		d.quit <- true
	}()
}

// QueueJob is used to send a job to a worker
func (d Dispatcher) QueueJob(ctx context.Context, data interface{}) {
	d.jobQueue <- job{
		ctx:  ctx,
		data: data,
	}
}

// Dispatchers is a slice of Dispatcher objects
type Dispatchers []Dispatcher

// QueueJob runs QueueJob on each Dispatcher in the slice
func (ds Dispatchers) QueueJob(ctx context.Context, data interface{}) {
	for _, d := range ds {
		d.QueueJob(ctx, data)
	}
}
