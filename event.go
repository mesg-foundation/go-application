package mesg

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/mesg-foundation/core/api/core"
)

var errCannotDecodeEventData = errors.New("cannot decode event data")

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

	// event is the actual event to listen for.
	event string

	//eventServiceID is the service id of where event is emitted.
	eventServiceID string

	// task is the actual task that will be executed.
	task string

	// taskServiceID is the service id of target task.
	taskServiceID string

	// filterFuncs holds funcs that returns boolean values to decide
	// if the task should be executed or not.
	filterFuncs []func(*Event) bool

	// mapFunc is a func that returns input data of task.
	mapFunc func(*Event) Data

	// m protects emitter configuration.
	m sync.Mutex

	// cancel cancels listening for upcoming events.
	cancel context.CancelFunc
}

// EventOption is the configuration func of EventListener.
type EventOption func(*EventEmitter)

// EventFilterOption returns a new option to filter events by name.
// Default is all(*).
func EventFilterOption(event string) EventOption {
	return func(l *EventEmitter) {
		l.event = event
	}
}

// WhenEvent creates an EventListener for serviceID.
func (a *Application) WhenEvent(serviceID string, options ...EventOption) *EventEmitter {
	e := &EventEmitter{
		app:            a,
		eventServiceID: serviceID,
		event:          "*",
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
func (e *EventEmitter) Filter(fn func(*Event) bool) *EventEmitter {
	e.m.Lock()
	defer e.m.Unlock()
	e.filterFuncs = append(e.filterFuncs, fn)
	return e
}

// Map sets the returned data as the input data of task.
// You can dynamically produce input values for task over event data.
func (e *EventEmitter) Map(fn func(*Event) Data) Executor {
	e.m.Lock()
	defer e.m.Unlock()
	e.mapFunc = fn
	return e
}

// Execute starts for listening events and executes task for serviceID with the
// output data of event or return value of Map if all Filter funcs returned as true.
func (e *EventEmitter) Execute(serviceID, task string) (*Listener, error) {
	e.taskServiceID = serviceID
	e.task = task
	listener := newListener()
	if err := e.app.startServices(e.eventServiceID, serviceID); err != nil {
		return nil, err
	}
	cancel, err := e.listen(listener)
	if err != nil {
		return nil, err
	}
	listener.cancel = cancel
	return listener, nil
}

// listen starts listening for events.
func (e *EventEmitter) listen(listener *Listener) (context.CancelFunc, error) {
	ctx, cancel := context.WithCancel(context.Background())
	resp, err := e.app.client.ListenEvent(ctx, &core.ListenEventRequest{
		ServiceID:   e.eventServiceID,
		EventFilter: e.event,
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
		data, err := resp.Recv()
		if err != nil {
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
	for _, filterFunc := range e.filterFuncs {
		if !filterFunc(event) {
			return
		}
	}

	var data Data
	if e.mapFunc != nil {
		data = e.mapFunc(event)
	} else if err := event.Data(&data); err != nil {
		e.app.log.Println(errCannotDecodeEventData)
		return
	}

	if _, err := e.app.execute(e.taskServiceID, e.task, data); err != nil {
		e.app.log.Println(executionError{e.task, err})
	}
}
