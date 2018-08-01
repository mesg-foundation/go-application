package mesg

import "context"

// Execution is a task exection.
type Execution struct {
	// ID is execution id of task.
	ID string

	// Err filled if an error occurs during task execution.
	Err error
}

// Stream is a task execution stream.
type Stream struct {
	// Executions filled with task executions.
	Executions chan *Execution

	// Err filled when stream fails to continue.
	Err chan error

	cancel context.CancelFunc
}

func newStream() *Stream {
	return &Stream{
		// lets make sure to not miss any Execution on the readers side
		// while initializing by making Executions chan buffered.
		Executions: make(chan *Execution, 100),
		Err:        make(chan error, 0),
	}
}

func (s *Stream) sendExecution(execution *Execution) {
	select {
	case s.Executions <- execution:
	default:
	}
}

func (s *Stream) sendError(err error) {
	s.Err <- err
}

// Close gracefully stops listening for events and shutdowns stream.
func (s *Stream) Close() error {
	s.cancel()
	return nil
}
