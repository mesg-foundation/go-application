package mesg

import "context"

// Listener is a task execution listener.
type Listener struct {
	// Err filled when Listener fails to continue.
	Err chan error

	cancel context.CancelFunc
}

func newListener() *Listener {
	return &Listener{
		Err: make(chan error, 1),
	}
}

func (l *Listener) sendError(err error) {
	l.Err <- err
}

// Close gracefully stops listening for events and shutdowns stream.
func (l *Listener) Close() error {
	l.cancel()
	return nil
}
