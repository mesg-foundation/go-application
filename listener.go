package mesg

import (
	"context"
	"sync"
)

// Listener is a task execution listener.
type Listener struct {
	// Err filled when Listener fails to continue.
	Err chan error

	// cancel stops receiving from gRPC stream.
	cancel context.CancelFunc

	app *Application

	// gracefulWait will be in the done state when all processing
	// events or results are done.
	gracefulWait *sync.WaitGroup
}

func newListener(app *Application, gracefulWait *sync.WaitGroup) *Listener {
	return &Listener{
		app:          app,
		gracefulWait: gracefulWait,
		Err:          make(chan error, 1),
	}
}

func (l *Listener) sendError(err error) {
	l.Err <- err
}

// Close gracefully waits current events or results to complete their process and
// stops listening for future events or results.
func (l *Listener) Close() error {
	l.app.removeListener(l)
	l.cancel()
	l.gracefulWait.Wait()
	return nil
}
