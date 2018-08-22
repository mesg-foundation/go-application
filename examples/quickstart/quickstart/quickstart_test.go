package quickstart

import (
	"io/ioutil"
	"sync"
	"testing"

	mesg "github.com/mesg-foundation/go-application"
	"github.com/mesg-foundation/go-application/mesgtest"
	"github.com/stvp/assert"
)

var config = Config{
	WebhookServiceID:    "x1",
	DiscordInvServiceID: "x2",
	LogServiceID:        "x3",
	SendgridKey:         "k1",
	Email:               "e1",
}

func newApplicationAndServer(t *testing.T) (*mesg.Application, *mesgtest.Server) {
	testServer := mesgtest.NewServer()
	application, err := mesg.New(
		mesg.DialOption(testServer.Socket()),
		mesg.LogOutputOption(ioutil.Discard),
	)
	assert.Nil(t, err)
	assert.NotNil(t, application)
	return application, testServer
}

func TestWhenRequest(t *testing.T) {
	app, server := newApplicationAndServer(t)
	quickstart := New(app, config, LogOutputOption(ioutil.Discard))

	go server.Start()
	go quickstart.Start()

	assert.Nil(t, server.EmitEvent(config.WebhookServiceID, "request", nil))

	le := <-server.LastExecute()
	assert.Equal(t, config.DiscordInvServiceID, le.ServiceID())
	assert.Equal(t, "send", le.Task())

	var data sendgridRequest
	assert.Nil(t, le.Data(&data))
	assert.Equal(t, config.SendgridKey, data.SendgridAPIKey)
	assert.Equal(t, config.Email, data.Email)
}

type logData struct {
	Info string `json:"info"`
}

func TestWhenResult(t *testing.T) {
	ldata := logData{"awesome log data"}

	app, server := newApplicationAndServer(t)
	quickStart := New(app, config, LogOutputOption(ioutil.Discard))

	go server.Start()
	go quickStart.Start()

	assert.Nil(t, server.EmitResult(config.DiscordInvServiceID, "send", "success", ldata, nil))

	le := <-server.LastExecute()

	assert.Equal(t, config.LogServiceID, le.ServiceID())
	assert.Equal(t, "log", le.Task())

	var data logRequest
	assert.Nil(t, le.Data(&data))
	assert.Equal(t, config.DiscordInvServiceID, data.ServiceID)
	assert.Equal(t, ldata.Info, data.Data.(map[string]interface{})["info"])
}

func TestWhenResultFalseFilter(t *testing.T) {
	var (
		trueFilterData  = logData{"awesome log data"}
		falseFilterData = "malformed json"
	)

	app, server := newApplicationAndServer(t)
	quickStart := New(app, config, LogOutputOption(ioutil.Discard))

	go server.Start()
	go quickStart.Start()

	// emit result with malformed data first.
	assert.Nil(t, server.EmitResult(config.DiscordInvServiceID, "send", "success", falseFilterData, nil))
	assert.Nil(t, server.EmitResult(config.DiscordInvServiceID, "send", "success", trueFilterData, nil))

	// at this point we're sure that first, result with falseFilterData and then
	// result with trueFilterData received by the application in order and task
	// execution made for result with trueFilterData.
	<-server.LastExecute()

	// gracefuly close quickstart app to make sure all on-going task executions has completed.
	quickStart.Close()

	select {
	// since false returned filter will not trigger a task execution we shouldn't get an
	// execution notification here.
	case <-server.LastExecute():
		t.Error("should not execute task because filter returns false")
	default:
	}
}

func TestClose(t *testing.T) {
	app, server := newApplicationAndServer(t)
	quickstart := New(app, config, LogOutputOption(ioutil.Discard))

	go server.Start()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NotNil(t, quickstart.Start())
	}()

	assert.Nil(t, server.EmitEvent(config.WebhookServiceID, "request", nil))
	// make sure server has been started.
	<-server.LastExecute()

	assert.Nil(t, quickstart.Close())
	wg.Wait()
}
