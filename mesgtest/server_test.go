package mesgtest

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/mesg-foundation/core/api/core"
	"github.com/stvp/assert"
)

func TestNewServer(t *testing.T) {
	server := NewServer()
	assert.NotNil(t, server)
	assert.NotNil(t, server.Socket())
}

func TestLastServiceStart(t *testing.T) {
	serviceID := "1"

	server := NewServer()

	server.core.StartService(context.Background(), &core.StartServiceRequest{
		ServiceID: serviceID,
	})

	ls := <-server.LastServiceStart()
	assert.Equal(t, serviceID, ls.ServiceID())
}

func TestLastServiceStartNonExistent(t *testing.T) {
	serviceID := "1"

	server := NewServer()
	server.MarkServiceAsNonExistent(serviceID)

	_, err := server.core.StartService(context.Background(), &core.StartServiceRequest{
		ServiceID: serviceID,
	})
	assert.Equal(t, ErrServiceDoesNotExists, err)

	select {
	case <-server.LastServiceStart():
		t.Error("should not start service")
	default:
	}
}

type eventData struct {
	URL string `json:"url"`
}

func TestLastEventListen(t *testing.T) {
	serviceID := "1"
	eventKey := "2"
	eventFilter := "3"
	data := eventData{"https://mesg.com"}
	dataStr := jsonMarshal(t, data)

	server := NewServer()
	stream := newEventDataStream()

	go server.core.ListenEvent(&core.ListenEventRequest{
		ServiceID:   serviceID,
		EventFilter: eventFilter,
	}, stream)

	go server.EmitEvent(serviceID, eventKey, data)

	eventData := <-stream.eventC
	assert.Equal(t, eventKey, eventData.EventKey)
	assert.Equal(t, dataStr, eventData.EventData)

	le := <-server.LastEventListen()
	assert.Equal(t, serviceID, le.ServiceID())
	assert.Equal(t, eventFilter, le.EventFilter())
}

type resultData struct {
	URL string `json:"url"`
}

func TestLastResultListen(t *testing.T) {
	serviceID := "1"
	taskKey := "2"
	taskFilter := "3"
	outputFilter := "4"
	outputKey := "5"
	data := resultData{"https://mesg.com"}
	dataStr := jsonMarshal(t, data)

	server := NewServer()
	stream := newResultDataStream()

	go server.core.ListenResult(&core.ListenResultRequest{
		ServiceID:    serviceID,
		TaskFilter:   taskFilter,
		OutputFilter: outputFilter,
	}, stream)

	go server.EmitResult(serviceID, taskKey, outputKey, data)

	resultData := <-stream.resultC
	assert.Equal(t, taskKey, resultData.TaskKey)
	assert.Equal(t, outputKey, resultData.OutputKey)
	assert.Equal(t, dataStr, resultData.OutputData)

	ll := <-server.LastResultListen()
	assert.Equal(t, serviceID, ll.ServiceID())
	assert.Equal(t, taskFilter, ll.TaskFilter())
	assert.Equal(t, outputFilter, ll.KeyFilter())
}

type taskExecute struct {
	URL string `json:"url"`
}

func TestLastExecute(t *testing.T) {
	serviceID := "1"
	taskKey := "2"
	data := taskExecute{"https://mesg.com"}
	dataStr := jsonMarshal(t, data)

	server := NewServer()

	server.core.ExecuteTask(context.Background(), &core.ExecuteTaskRequest{
		ServiceID: serviceID,
		TaskKey:   taskKey,
		InputData: dataStr,
	})

	le := <-server.LastExecute()
	assert.Equal(t, serviceID, le.ServiceID())
	assert.Equal(t, taskKey, le.Task())

	var data1 taskExecute
	assert.Nil(t, le.Data(&data1))
	assert.Equal(t, data.URL, data1.URL)
}

func jsonMarshal(t *testing.T, data interface{}) string {
	bytes, err := json.Marshal(data)
	assert.Nil(t, err)
	return string(bytes)
}
