package mesg

import (
	"fmt"
)

// Executor is a task executor for emitters.
type Executor interface {
	Execute(serviceID, task string) (*Listener, error)
}

type executionError struct {
	task string
	err  error
}

func (e executionError) Error() string {
	return fmt.Sprintf("task `%s` execution complated with an error: %s", e.task, e.err)
}
