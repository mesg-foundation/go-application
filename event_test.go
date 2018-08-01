package mesg

import (
	"sync"
	"testing"

	"github.com/stvp/assert"
)

const endpoint = "endpoint"

func TestWhenEvent(t *testing.T) {
	eventServiceID := "1"
	taskServiceID := "2"
	task := "3"
	taskData := taskRequest{"https://mesg.com"}

	app, server := newApplicationAndServer(t)
	go server.Start()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		stream, err := app.
			WhenEvent(eventServiceID).
			Map(func(*Event) Data {
				return taskData
			}).
			Execute(taskServiceID, task)
		assert.Nil(t, err)
		assert.NotNil(t, stream)
	}()

	el := server.LastEventListen()
	assert.Equal(t, eventServiceID, el.ServiceID())
	assert.Equal(t, "*", el.EventFilter())

	wg.Wait()
}

func TestWhenEventWithEventFilter(t *testing.T) {
	eventServiceID := "x1"
	taskServiceID := "x2"
	task := "send"
	event := "request"
	taskData := taskRequest{"https://mesg.com"}

	app, server := newApplicationAndServer(t)
	go server.Start()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		stream, err := app.
			WhenEvent(eventServiceID, EventFilterOption(event)).
			Map(func(*Event) Data {
				return taskData
			}).
			Execute(taskServiceID, task)
		assert.Nil(t, err)
		assert.NotNil(t, stream)
	}()

	el := server.LastEventListen()
	assert.Equal(t, eventServiceID, el.ServiceID())
	assert.Equal(t, event, el.EventFilter())

	wg.Wait()
}

func TestWhenEventServiceStart(t *testing.T) {
	eventServiceID := "1"
	taskData := taskRequest{"https://mesg.com"}
	taskServiceID := "2"
	task := "3"

	app, server := newApplicationAndServer(t)
	go server.Start()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		lastStartIDs := []string{
			server.LastServiceStart().ServiceID(),
			server.LastServiceStart().ServiceID(),
		}

		assert.True(t, stringSliceContains(lastStartIDs, eventServiceID))
		assert.True(t, stringSliceContains(lastStartIDs, taskServiceID))
	}()

	app.WhenEvent(eventServiceID).
		Map(func(*Event) Data {
			return taskData
		}).
		Execute(taskServiceID, task)

	wg.Wait()
}
func TestWhenEventServiceStartError(t *testing.T) {
	eventServiceID := "1"
	taskData := taskRequest{"https://mesg.com"}
	taskServiceID := "non-existent-task-service"
	task := "3"

	app, server := newApplicationAndServer(t)
	go server.Start()
	server.MarkServiceAsNonExistent(taskServiceID)

	stream, err := app.WhenEvent(eventServiceID).
		Map(func(*Event) Data {
			return taskData
		}).
		Execute(taskServiceID, task)
	assert.NotNil(t, err)
	assert.Nil(t, stream)
}

type taskRequest struct {
	URL string `json:"url"`
}

type eventData struct {
	URL string `json:"url"`
}

func TestWhenEventExecute(t *testing.T) {
	eventServiceID := "1"
	taskServiceID := "2"
	task := "3"
	event := "4"
	reqData := taskRequest{"https://mesg.tech"}
	evData := eventData{"https://mesg.com"}

	app, server := newApplicationAndServer(t)
	go server.Start()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()

		e := server.LastExecute()
		assert.Equal(t, taskServiceID, e.ServiceID())
		assert.Equal(t, task, e.Task())

		var data taskRequest
		assert.Nil(t, e.Data(&data))
		assert.Equal(t, reqData.URL, data.URL)
	}()

	wg.Add(1)
	stream, err := app.
		WhenEvent(eventServiceID).
		Filter(func(event *Event) bool {
			defer wg.Done()
			var data eventData
			assert.Nil(t, event.Data(&data))
			assert.Equal(t, evData.URL, data.URL)
			return true
		}).
		Map(func(*Event) Data {
			return reqData
		}).
		Execute(taskServiceID, task)

	assert.Nil(t, err)
	assert.NotNil(t, stream)

	go server.EmitEvent(eventServiceID, event, evData)

	execution := <-stream.Executions
	assert.Nil(t, execution.Err)
	assert.True(t, execution.ID != "")

	wg.Wait()
}

func TestWhenEventClose(t *testing.T) {
	eventServiceID := "1"
	taskServiceID := "2"
	task := "3"
	taskData := taskRequest{"https://mesg.com"}
	event := "4"
	evData := eventData{"https://mesg.com"}

	app, server := newApplicationAndServer(t)
	go server.Start()
	go server.EmitEvent(eventServiceID, event, evData)

	stream, err := app.
		WhenEvent(eventServiceID).
		Map(func(*Event) Data {
			return taskData
		}).
		Execute(taskServiceID, task)

	assert.Nil(t, err)
	assert.NotNil(t, stream)

	stream.Close()
	assert.NotNil(t, <-stream.Err)
}

func stringSliceContains(s []string, e string) bool {
	for _, a := range s {
		if a == e {
			return true
		}
	}
	return false
}
