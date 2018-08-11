package mesg

import (
	"io/ioutil"
	"testing"

	"github.com/mesg-foundation/go-application/mesgtest"
	"github.com/stvp/assert"
)

func newApplicationAndServer(t *testing.T) (*Application, *mesgtest.Server) {
	testServer := mesgtest.NewServer()

	app, err := New(
		DialOption(testServer.Socket()),
		EndpointOption(endpoint),
		LogOutputOption(ioutil.Discard),
	)

	assert.Nil(t, err)
	assert.NotNil(t, app)

	return app, testServer
}

func TestExecute(t *testing.T) {
	serviceID := "1"
	task := "2"
	reqData := taskRequest{"https://mesg.tech"}

	app, server := newApplicationAndServer(t)
	go server.Start()

	executionID, err := app.execute(serviceID, task, reqData)
	assert.Nil(t, err)
	assert.True(t, executionID != "")

	execution := <-server.LastExecute()

	assert.Equal(t, serviceID, execution.ServiceID())
	assert.Equal(t, task, execution.Task())

	var data taskRequest
	assert.Nil(t, execution.Data(&data))
	assert.Equal(t, reqData.URL, data.URL)
}
