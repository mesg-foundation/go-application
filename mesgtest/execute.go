package mesgtest

import "encoding/json"

// Execute holds information about a task execution.
type Execute struct {
	serviceID     string
	task          string
	data          string
	executionTags []string
}

// ServiceID returns the id of service that task executed on.
func (e *Execute) ServiceID() string {
	return e.serviceID
}

// Task returns the executed task's name.
func (e *Execute) Task() string {
	return e.task
}

// Tags returns the set execution tags for execution.
func (e *Execute) Tags() []string {
	return e.executionTags
}

// Data decodes task's input data to out.
func (e *Execute) Data(out interface{}) error {
	return json.Unmarshal([]byte(e.data), out)
}
