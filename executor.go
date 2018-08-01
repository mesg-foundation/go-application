package mesg

// Executor is a task executor for emitters.
type Executor struct {
	eventEmitter  *EventEmitter
	resultEmitter *ResultEmitter
}

func newEventEmitterExecutor(e *EventEmitter) *Executor {
	return &Executor{eventEmitter: e}
}

func newResultEmitterExecutor(e *ResultEmitter) *Executor {
	return &Executor{resultEmitter: e}
}

// Execute executes tasks on event or results for serviceID.
// Input Data of task retrieved from the return value of Map func of emitter.
func (e *Executor) Execute(serviceID, task string) (*Stream, error) {
	if e.eventEmitter != nil {
		return e.eventEmitter.start(serviceID, task)
	}
	return e.resultEmitter.start(serviceID, task)
}
