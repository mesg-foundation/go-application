package mesg

// Executor is a task executor for emitters.
type Executor struct {
	starter starter
}

// starter is an event/result based task execution starter.
type starter interface {
	start(serviceID, task string) (*Stream, error)
}

func newExecutor(s starter) *Executor {
	return &Executor{starter: s}
}

// Execute executes tasks on event or results for serviceID.
// Input Data of task retrieved from the return value of Map func of emitter.
func (e *Executor) Execute(serviceID, task string) (*Stream, error) {
	return e.starter.start(serviceID, task)
}
