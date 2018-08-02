package mesg

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/mesg-foundation/core/api/core"
)

var errCannotDecodeResultData = errors.New("cannot decode result data")

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
	m sync.Mutex

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
func (e *ResultEmitter) Map(fn func(*Result) Data) Executor {
	e.m.Lock()
	defer e.m.Unlock()
	e.mapFunc = fn
	return e
}

// Execute starts for listening events and executes task for serviceID with the
// output data of result or return value of Map if all Filter funcs returned as true.
func (e *ResultEmitter) Execute(serviceID, task string) (*Listener, error) {
	e.taskServiceID = serviceID
	e.task = task
	listener := newListener()
	if err := e.app.startServices(e.taskServiceID, serviceID); err != nil {
		return nil, err
	}
	cancel, err := e.listen(listener)
	if err != nil {
		return nil, err
	}
	listener.cancel = cancel
	return listener, nil
}

// listen starts listening for results.
func (e *ResultEmitter) listen(listener *Listener) (context.CancelFunc, error) {
	ctx, cancel := context.WithCancel(context.Background())
	resp, err := e.app.client.ListenResult(ctx, &core.ListenResultRequest{
		ServiceID:    e.resultServiceID,
		TaskFilter:   e.resultTask,
		OutputFilter: e.outputKey,
	})
	if err != nil {
		return cancel, err
	}
	go e.readStream(listener, resp)
	return cancel, nil
}

// readStream reads listen result stream.
func (e *ResultEmitter) readStream(listener *Listener, resp core.Core_ListenResultClient) {
	for {
		data, err := resp.Recv()
		if err != nil {
			listener.sendError(err)
			return
		}
		result := &Result{
			TaskKey:     data.TaskKey,
			OutputKey:   data.OutputKey,
			data:        data.OutputData,
			executionID: data.ExecutionID,
		}
		go e.execute(listener, result)
	}
}

// execute executes the task with data returned from Map if all filters
// are met.
func (e *ResultEmitter) execute(listener *Listener, result *Result) {
	for _, filterFunc := range e.filterFuncs {
		if !filterFunc(result) {
			return
		}
	}

	var data Data
	if e.mapFunc != nil {
		data = e.mapFunc(result)
	} else if err := result.Data(&data); err != nil {
		e.app.log.Println(errCannotDecodeResultData)
		return
	}

	if _, err := e.app.execute(e.taskServiceID, e.task, data); err != nil {
		e.app.log.Println(executionError{e.task, err})
	}
}
