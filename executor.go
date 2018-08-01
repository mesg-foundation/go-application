package mesg

// Executor is a task executor for emitters.
type Executor interface {
	Execute(serviceID, task string) (*Stream, error)
}
