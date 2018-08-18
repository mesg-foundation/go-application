package mesg

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/mesg-foundation/core/api/core"
)

// Event is a MESG event.
type Event struct {
	Key  string
	data string
}

// Data decodes event data into out.
func (e *Event) Data(out interface{}) error {
	return json.Unmarshal([]byte(e.data), out)
}

// EventEmitter is a MESG event emitter.
type EventEmitter struct {
	app *Application

	// eventKey is the actual event to listen for.
	eventKey string

	//eventServiceID is the service id of where event is emitted.
	eventServiceID string

	// taskKey is the actual task that will be executed.
	taskKey string

	// taskServiceID is the service id of target task.
	taskServiceID string

	// filterFuncs holds funcs that returns boolean values to decide
	// if the task should be executed or not.
	filterFuncs []func(*Event) bool

	// mapFunc is a func that returns input data of task.
	mapFunc func(*Event) Data

	// executionTagsFunc is a func that returns execution tags filter.
	executionTagsFunc func(*Event) []string

	// gracefulWait will be in the done state when all processing
	// events are done.
	gracefulWait *sync.WaitGroup
}

// EventCondition is the condition configurator for filtering events.
type EventCondition func(*EventEmitter)

// EventKeyCondition returns a new condition to filter events by name.
// Default is all(*).
func EventKeyCondition(event string) EventCondition {
	return func(l *EventEmitter) {
		l.eventKey = event
	}
}

// WhenEvent creates an EventEmitter for serviceID.
func (a *Application) WhenEvent(serviceID string, conditions ...EventCondition) *EventEmitter {
	e := &EventEmitter{
		app:            a,
		eventServiceID: serviceID,
		eventKey:       "*",
		gracefulWait:   &sync.WaitGroup{},
	}
	for _, condition := range conditions {
		condition(e)
	}
	return e
}

// Filter sets filter funcs that will be executed to decide to execute the
// task or not.
// It's possible to add multiple filters by calling Filter multiple times.
// Other filter funcs and the task execution will no proceed if a filter
// func returns false.
func (e *EventEmitter) Filter(fn func(*Event) (execute bool)) *EventEmitter {
	e.filterFuncs = append(e.filterFuncs, fn)
	return e
}

// SetTags sets execution tags for task executions.
func (e *EventEmitter) SetTags(fn func(*Event) (tags []string)) *EventEmitter {
	e.executionTagsFunc = fn
	return e
}

// Map sets the returned data as the input data of task.
// You can dynamically produce input values for task over event data.
func (e *EventEmitter) Map(fn func(*Event) Data) Executor {
	e.mapFunc = fn
	return e
}

// Execute starts for listening events and executes task for serviceID with the
// output data of event or return value of Map if all Filter funcs returned as true.
func (e *EventEmitter) Execute(serviceID, task string) (*Listener, error) {
	e.taskServiceID = serviceID
	e.taskKey = task
	listener := newListener(e.app, e.gracefulWait)
	if err := e.app.startServices(e.eventServiceID, serviceID); err != nil {
		return nil, err
	}
	cancel, err := e.listen(listener)
	if err != nil {
		return nil, err
	}
	listener.cancel = cancel
	e.app.addListener(listener)
	return listener, nil
}

// listen starts listening for events.
func (e *EventEmitter) listen(listener *Listener) (context.CancelFunc, error) {
	ctx, cancel := context.WithCancel(context.Background())
	resp, err := e.app.client.ListenEvent(ctx, &core.ListenEventRequest{
		ServiceID:   e.eventServiceID,
		EventFilter: e.eventKey,
	})
	if err != nil {
		return cancel, err
	}
	go e.readStream(listener, resp)
	return cancel, nil
}

// readStream reads listen result stream.
func (e *EventEmitter) readStream(listener *Listener, resp core.Core_ListenEventClient) {
	for {
		e.gracefulWait.Add(1)
		data, err := resp.Recv()
		if err != nil {
			e.gracefulWait.Done()
			listener.sendError(err)
			return
		}
		event := &Event{
			Key:  data.EventKey,
			data: data.EventData,
		}
		go e.execute(listener, event)
	}
}

// execute executes the task with data returned from Map if all filters
// are met.
func (e *EventEmitter) execute(listener *Listener, event *Event) {
	defer e.gracefulWait.Done()

	for _, filterFunc := range e.filterFuncs {
		if !filterFunc(event) {
			return
		}
	}

	var (
		data          Data
		executionTags []string
	)

	if e.mapFunc != nil {
		data = e.mapFunc(event)
	} else if err := event.Data(&data); err != nil {
		e.app.log.Println(errDecodingEventData{err})
		return
	}

	if e.executionTagsFunc != nil {
		executionTags = e.executionTagsFunc(event)
	}

	if _, err := e.app.execute(e.taskServiceID, e.taskKey, data, executionTags); err != nil {
		e.app.log.Println(executionError{e.taskKey, err})
	}
}

type errDecodingEventData struct {
	err error
}

func (e errDecodingEventData) Error() string {
	return fmt.Sprintf("cannot decode event data err: %s", e.err)
}
