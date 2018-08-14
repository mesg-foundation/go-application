package mesg

import (
	"sync"
	"testing"

	"github.com/stvp/assert"
)

type taskResult struct {
	URL string `json:"url"`
}

func TestWhenResult(t *testing.T) {
	resultServiceID := "1"
	taskResultData := taskResult{"https://mesg.tech"}
	taskExecuteData := taskRequest{"https://mesg.com"}
	taskServiceID := "2"
	task := "3"
	task1 := "4"
	outputKey := "5"

	app, server := newApplicationAndServer(t)
	go server.Start()

	var wg sync.WaitGroup

	wg.Add(1)
	stream, err := app.
		WhenResult(resultServiceID).
		Map(func(result *Result) Data {
			defer wg.Done()

			assert.Equal(t, task, result.TaskKey)
			assert.Equal(t, outputKey, result.OutputKey)

			var data taskResult
			assert.Nil(t, result.Data(&data))
			assert.Equal(t, taskResultData.URL, data.URL)

			return taskExecuteData
		}).
		Execute(taskServiceID, task1)

	assert.Nil(t, err)
	assert.NotNil(t, stream)

	server.EmitResult(resultServiceID, task, outputKey, taskResultData)

	ll := <-server.LastResultListen()
	assert.Equal(t, resultServiceID, ll.ServiceID())
	assert.Equal(t, "*", ll.KeyFilter())
	assert.Equal(t, "*", ll.TaskFilter())

	wg.Wait()
}

func TestWhenResultExecute(t *testing.T) {
	resultServiceID := "1"
	taskResultData := taskResult{"https://mesg.tech"}
	taskExecuteData := taskRequest{"https://mesg.com"}
	taskServiceID := "2"
	task := "3"
	task1 := "4"
	outputKey := "5"

	app, server := newApplicationAndServer(t)
	go server.Start()

	app.
		WhenResult(resultServiceID).
		Map(func(result *Result) Data {
			return taskExecuteData
		}).
		Execute(taskServiceID, task1)

	server.EmitResult(resultServiceID, task, outputKey, taskResultData)

	le := <-server.LastExecute()

	assert.Equal(t, taskServiceID, le.ServiceID())
	assert.Equal(t, task1, le.Task())

	var data taskRequest
	assert.Nil(t, le.Data(&data))
	assert.Equal(t, taskExecuteData.URL, data.URL)
}

func TestWhenResultServiceStart(t *testing.T) {
	resultServiceID := "1"
	taskData := taskRequest{"https://mesg.com"}
	taskServiceID := "2"
	task := "3"

	app, server := newApplicationAndServer(t)
	go server.Start()

	_, err := app.WhenResult(resultServiceID).
		Map(func(*Result) Data {
			return taskData
		}).
		Execute(taskServiceID, task)

	assert.Nil(t, err)

	lastStartIDs := []string{
		(<-server.LastServiceStart()).ServiceID(),
		(<-server.LastServiceStart()).ServiceID(),
	}

	assert.True(t, stringSliceContains(lastStartIDs, resultServiceID))
	assert.True(t, stringSliceContains(lastStartIDs, taskServiceID))
}
