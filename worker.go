package workerpool

// worker represents the worker that executes the job
type worker struct {
	id         int
	jobCh      chan job
	quit       chan bool
	dispatcher Dispatcher
}

func newWorker(d Dispatcher, id int) worker {
	return worker{
		id:         id,
		jobCh:      make(chan job),
		quit:       make(chan bool),
		dispatcher: d,
	}
}

// Start method starts the run loop for the worker, listening for a quit channel in
// case we need to stop it
func (w worker) Start() worker {
	go func() {
		for {
			// register the current worker into the worker queue.
			w.dispatcher.jobChannels <- w.jobCh

			select {
			case j := <-w.jobCh:
				// we have received a work request.
				if j.ctx.Err() == nil {
					w.dispatcher.handler.Do(j.ctx, j.data)
				}
			case <-w.quit:
				// we have received a signal to stop
				return
			}
		}
	}()

	return w
}

// Stop signals the worker to stop listening for work requests.
func (w worker) Stop() {
	go func() {
		w.quit <- true
	}()
}
