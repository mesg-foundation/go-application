package mesg

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/mesg-foundation/core/api/core"
)

// Result is MESG result event.
type Result struct {
	TaskKey     string
	OutputKey   string
	data        string
	executionID string
}

// Data decodes result data into out.
func (e *Result) Data(out interface{}) error {
	return json.Unmarshal([]byte(e.data), out)
}

// ResultEmitter is a MESG result event listener.
type ResultEmitter struct {
	app *Application

	// resultTask is the actual event to listen for.
	resultTask string

	//resultServiceID is the service id of where result is emitted.
	resultServiceID string

	// outputKey is the output key to listen for.
	outputKey string

	// task is the actual task that will be executed.
	task string

	// taskServiceID is the service id of target task.
	taskServiceID string

	// filterFuncs holds funcs that returns boolean values to decide
	// if the task should be executed or not.
	filterFuncs []func(*Result) bool

	// mapFunc is a func that returns input data of task.
	mapFunc func(*Result) Data

	// m protects emitter configuration.
	m sync.RWMutex

	// cancel cancels listening for upcoming events.
	cancel context.CancelFunc
}

// ResultOption is the configuration func of ResultEmitter.
type ResultOption func(*ResultEmitter)

// TaskFilterOption returns a new option to filter results by task name.
// Default is all(*).
func TaskFilterOption(task string) ResultOption {
	return func(l *ResultEmitter) {
		l.resultTask = task
	}
}

// OutputKeyFilterOption returns a new option to filter results by output key name.
// Default is all(*).
func OutputKeyFilterOption(key string) ResultOption {
	return func(l *ResultEmitter) {
		l.outputKey = key
	}
}

// WhenResult creates a ResultEmitter for serviceID.
func (a *Application) WhenResult(serviceID string, options ...ResultOption) *ResultEmitter {
	e := &ResultEmitter{
		app:             a,
		resultServiceID: serviceID,
		resultTask:      "*",
		outputKey:       "*",
	}
	for _, option := range options {
		option(e)
	}
	return e
}

// Filter sets filter funcs that will be executed to decide to execute the
// task or not.
// It's possible to add multiple filters by calling Filter multiple times.
// Other filter funcs and the task execution will no proceed if a filter
// func returns false.
func (e *ResultEmitter) Filter(fn func(*Result) bool) *ResultEmitter {
	e.m.Lock()
	defer e.m.Unlock()
	e.filterFuncs = append(e.filterFuncs, fn)
	return e
}

// Map sets the returned data as the input data of task.
// You can dynamically produce input values for task over result data.
func (e *ResultEmitter) Map(fn func(*Result) Data) *Executor {
	e.m.Lock()
	defer e.m.Unlock()
	e.mapFunc = fn
	return newExecutor(e)
}

// Execute executes task for serviceID on each received result.
// Input data of task retrieved from the output data of result.
func (e *ResultEmitter) Execute(serviceID, task string) (*Stream, error) {
	return newExecutor(e).Execute(serviceID, task)
}

// start starts for listening results and executes task on serviceID when
// a result received and all Filter funcs returned true.
func (e *ResultEmitter) start(serviceID, task string) (*Stream, error) {
	e.taskServiceID = serviceID
	e.task = task
	stream := &Stream{
		Executions: make(chan *Execution, 0),
		Err:        make(chan error, 0),
	}
	if err := e.app.startServices(e.taskServiceID, serviceID); err != nil {
		return nil, err
	}
	cancel, err := e.listen(stream)
	if err != nil {
		return nil, err
	}
	stream.cancel = cancel
	return stream, nil
}

// listen starts listening for results.
func (e *ResultEmitter) listen(stream *Stream) (context.CancelFunc, error) {
	ctx, cancel := context.WithCancel(context.Background())
	resp, err := e.app.client.ListenResult(ctx, &core.ListenResultRequest{
		ServiceID:    e.resultServiceID,
		TaskFilter:   e.resultTask,
		OutputFilter: e.outputKey,
	})
	if err != nil {
		return cancel, err
	}
	go e.readStream(stream, resp)
	return cancel, nil
}

// readStream reads listen result stream.
func (e *ResultEmitter) readStream(stream *Stream, resp core.Core_ListenResultClient) {
	for {
		data, err := resp.Recv()
		if err != nil {
			stream.Err <- err
			return
		}
		result := &Result{
			TaskKey:     data.TaskKey,
			OutputKey:   data.OutputKey,
			data:        data.OutputData,
			executionID: data.ExecutionID,
		}
		go e.execute(stream, result)
	}
}

// execute executes the task with data returned from Map if all filters
// are met.
func (e *ResultEmitter) execute(stream *Stream, result *Result) {
	e.m.RLock()
	for _, filterFunc := range e.filterFuncs {
		if !filterFunc(result) {
			e.m.RUnlock()
			return
		}
	}
	e.m.RUnlock()

	var data Data
	if e.mapFunc != nil {
		data = e.mapFunc(result)
	} else if err := result.Data(&data); err != nil {
		stream.Executions <- &Execution{
			Err: err,
		}
		return
	}

	executionID, err := e.app.execute(e.taskServiceID, e.task, data)
	stream.Executions <- &Execution{
		ID:  executionID,
		Err: err,
	}
}
