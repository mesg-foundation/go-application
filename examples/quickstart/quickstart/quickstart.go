package quickstart

import (
	"io"
	"log"
	"os"

	mesg "github.com/mesg-foundation/go-application"
)

// Option is a configuration func for QuickStart.
type Option func(*QuickStart)

// QuickStart represents a quick-start application.
type QuickStart struct {
	// app is a MESG application
	app *mesg.Application

	config Config

	// listeners holds MESG's application listeners
	listeners []*mesg.Listener
	errC      chan error
	isClosed  bool

	log       *log.Logger
	logOutput io.Writer
}

// Config holds the application configuration.
type Config struct {
	WebhookServiceID    string
	DiscordInvServiceID string
	LogServiceID        string
	SendgridKey         string
	Email               string
}

// New creates a new QuickStart application.
func New(app *mesg.Application, config Config, options ...Option) *QuickStart {
	q := &QuickStart{
		app:       app,
		config:    config,
		errC:      make(chan error, 1),
		logOutput: os.Stdout,
	}
	for _, option := range options {
		option(q)
	}
	q.log = log.New(q.logOutput, "quick-start", log.LstdFlags)
	return q
}

// LogOutputOption uses out as a log destination.
func LogOutputOption(out io.Writer) Option {
	return func(q *QuickStart) {
		q.logOutput = out
	}
}

// Start starts quickstart application.
func (q *QuickStart) Start() error {
	defer q.app.Close()

	q.monitor(q.whenRequest())
	q.monitor(q.whenDiscordSend())

	return q.wait()
}

func (q *QuickStart) monitor(listener *mesg.Listener, err error) {
	if q.isClosed {
		return
	}
	if err != nil {
		q.isClosed = true
		q.errC <- err
		return
	}
	q.listeners = append(q.listeners, listener)
}

func (q *QuickStart) wait() error {
	select {
	case err := <-q.errC:
		return err
	default:
	}

	listenersLen := len(q.listeners)
	errC := make(chan error, listenersLen)

	for _, listener := range q.listeners {
		go q.monitorListener(listener, errC)
	}

	for i := 0; i < listenersLen; i++ {
		if err := <-errC; err != nil {
			return err
		}
	}

	return nil
}

func (q *QuickStart) monitorListener(listener *mesg.Listener, errC chan error) {
	for {
		select {
		case errC <- <-listener.Err:
			return
		}
	}
}

// Close gracefully closes quickstart application.
func (q *QuickStart) Close() error {
	return q.app.Close()
}
