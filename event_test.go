package mesg

import (
	"sync"
	"testing"

	"github.com/mesg-foundation/go-application/mesgtest"
	"github.com/stvp/assert"
)

const endpoint = "endpoint"

func TestWhenEvent(t *testing.T) {
	var (
		eventServiceID = "1"
		taskServiceID  = "2"
		task           = "3"
		taskData       = taskRequest{"https://mesg.com"}
	)

	app, server := newApplicationAndServer(t)
	go server.Start()

	listener, err := app.
		WhenEvent(eventServiceID).
		Map(func(*Event) Data {
			return taskData
		}).
		Execute(taskServiceID, task)

	assert.Nil(t, err)
	assert.NotNil(t, listener)

	ll := <-server.LastEventListen()
	assert.Equal(t, eventServiceID, ll.ServiceID())
	assert.Equal(t, "*", ll.EventFilter())
}

func TestWhenEventWithEventFilter(t *testing.T) {
	var (
		eventServiceID = "x1"
		taskServiceID  = "x2"
		task           = "send"
		event          = "request"
		taskData       = taskRequest{"https://mesg.com"}
	)

	app, server := newApplicationAndServer(t)
	go server.Start()

	listener, err := app.
		WhenEvent(eventServiceID, EventKeyCondition(event)).
		Map(func(*Event) Data {
			return taskData
		}).
		Execute(taskServiceID, task)

	assert.Nil(t, err)
	assert.NotNil(t, listener)

	ll := <-server.LastEventListen()
	assert.Equal(t, eventServiceID, ll.ServiceID())
	assert.Equal(t, event, ll.EventFilter())
}

func TestWhenEventServiceStart(t *testing.T) {
	var (
		eventServiceID = "1"
		taskData       = taskRequest{"https://mesg.com"}
		taskServiceID  = "2"
		task           = "3"
	)

	app, server := newApplicationAndServer(t)
	go server.Start()

	_, err := app.WhenEvent(eventServiceID).
		Map(func(*Event) Data {
			return taskData
		}).
		Execute(taskServiceID, task)

	assert.Nil(t, err)

	lastStartIDs := []string{
		(<-server.LastServiceStart()).ServiceID(),
		(<-server.LastServiceStart()).ServiceID(),
	}

	assert.True(t, stringSliceContains(lastStartIDs, eventServiceID))
	assert.True(t, stringSliceContains(lastStartIDs, taskServiceID))
}

func TestWhenEventServiceStartError(t *testing.T) {
	var (
		eventServiceID = "1"
		taskData       = taskRequest{"https://mesg.com"}
		taskServiceID  = "non-existent-task-service"
		task           = "3"
	)

	app, server := newApplicationAndServer(t)

	go server.Start()
	server.MarkServiceAsNonExistent(taskServiceID)

	listener, err := app.WhenEvent(eventServiceID).
		Map(func(*Event) Data {
			return taskData
		}).
		Execute(taskServiceID, task)

	assert.Contains(t, mesgtest.ErrServiceDoesNotExists.Error(), err.Error())
	assert.Nil(t, listener)
}

type taskRequest struct {
	URL string `json:"url"`
}

type eventData struct {
	URL string `json:"url"`
}

func TestWhenEventMapExecute(t *testing.T) {
	var (
		eventServiceID = "1"
		taskServiceID  = "2"
		task           = "3"
		event          = "4"
		reqData        = taskRequest{"https://mesg.tech"}
		evData         = eventData{"https://mesg.com"}
	)

	app, server := newApplicationAndServer(t)
	go server.Start()

	var wg sync.WaitGroup

	wg.Add(2)

	listener, err := app.
		WhenEvent(eventServiceID).
		Filter(func(event *Event) bool {
			defer wg.Done()

			var data eventData
			assert.Nil(t, event.Data(&data))
			assert.Equal(t, evData.URL, data.URL)

			return true
		}).
		Map(func(*Event) Data {
			wg.Done()
			return reqData
		}).
		Execute(taskServiceID, task)

	assert.Nil(t, err)
	assert.NotNil(t, listener)

	server.EmitEvent(eventServiceID, event, evData)

	le := <-server.LastExecute()

	assert.Equal(t, taskServiceID, le.ServiceID())
	assert.Equal(t, task, le.Task())

	var data taskRequest
	assert.Nil(t, le.Data(&data))
	assert.Equal(t, reqData.URL, data.URL)

	wg.Wait()
}

func TestWhenEventClose(t *testing.T) {
	var (
		eventServiceID = "1"
		taskServiceID  = "2"
		task           = "3"
		taskData       = taskRequest{"https://mesg.com"}
		event          = "4"
		evData         = eventData{"https://mesg.com"}
	)

	app, server := newApplicationAndServer(t)
	go server.Start()

	listener, err := app.
		WhenEvent(eventServiceID).
		Map(func(*Event) Data {
			return taskData
		}).
		Execute(taskServiceID, task)

	assert.Nil(t, err)
	assert.NotNil(t, listener)

	server.EmitEvent(eventServiceID, event, evData)

	listener.Close()
	assert.NotNil(t, <-listener.Err)
}

func TestWhenEventExecute(t *testing.T) {
	var (
		eventServiceID = "1"
		taskServiceID  = "2"
		task           = "3"
		event          = "4"
		evData         = eventData{"https://mesg.com"}
	)

	app, server := newApplicationAndServer(t)
	go server.Start()

	listener, err := app.
		WhenEvent(eventServiceID).
		Execute(taskServiceID, task)

	assert.Nil(t, err)
	assert.NotNil(t, listener)

	server.EmitEvent(eventServiceID, event, evData)

	le := <-server.LastExecute()

	assert.Equal(t, taskServiceID, le.ServiceID())
	assert.Equal(t, task, le.Task())

	var data taskRequest
	assert.Nil(t, le.Data(&data))
	assert.Equal(t, evData.URL, data.URL)
}

func TestAddAndRemoveListener(t *testing.T) {
	var (
		eventServiceID = "1"
		taskServiceID  = "2"
		taskKey        = "3"
	)

	app, server := newApplicationAndServer(t)
	go server.Start()

	ln, _ := app.
		WhenEvent(eventServiceID).
		Execute(taskServiceID, taskKey)

	assert.Equal(t, 1, len(app.listeners))

	ln.Close()
	assert.Equal(t, 0, len(app.listeners))
}

func TestWhenEventSetTags(t *testing.T) {
	var (
		eventServiceID  = "1"
		taskEventData   = eventData{"https://mesg.tech"}
		taskExecuteData = taskRequest{"https://mesg.com"}
		taskServiceID   = "2"
		event           = "3"
		task            = "4"
		setTags         = []string{"tag-1", "tag-2"}
	)

	app, server := newApplicationAndServer(t)
	go server.Start()

	app.
		WhenEvent(eventServiceID).
		SetTags(func(event *Event) []string {
			return setTags
		}).
		Map(func(event *Event) Data {
			return taskExecuteData
		}).
		Execute(taskServiceID, task)

	server.EmitEvent(eventServiceID, event, taskEventData)

	ll := <-server.LastExecute()
	assert.Equal(t, setTags, ll.Tags())
}

func stringSliceContains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
