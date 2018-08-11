// Package mesg is an application client for mesg-core.
// For more information please visit https://mesg.com.
package mesg

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/mesg-foundation/core/api/core"
	"google.golang.org/grpc"
)

const (
	endpointEnv     = "MESG_ENDPOINT"
	defaultEndpoint = "localhost:50052"
)

// Application represents is a MESG application.
type Application struct {
	// endpoint is MESG's network address.
	endpoint string

	// Client is the gRPC core client of MESG.
	client core.CoreClient
	conn   *grpc.ClientConn

	// callTimeout used to set timeout for gRPC requests.
	callTimeout time.Duration

	// dialOptions holds dial options of gRPC.
	dialOptions []grpc.DialOption

	listeners []*Listener
	lm        sync.Mutex

	log       *log.Logger
	logOutput io.Writer
}

// Option is the configuration func of Application.
type Option func(*Application)

// New returns a new Application with options.
func New(options ...Option) (*Application, error) {
	a := &Application{
		endpoint:    os.Getenv(endpointEnv),
		callTimeout: time.Second * 10,
		logOutput:   os.Stdout,
		dialOptions: []grpc.DialOption{grpc.WithInsecure()},
	}
	for _, option := range options {
		option(a)
	}
	a.log = log.New(a.logOutput, "mesg", log.LstdFlags)
	if a.endpoint == "" {
		a.endpoint = defaultEndpoint
	}
	return a, a.setupCoreClient()
}

// EndpointOption receives the endpoint of MESG.
func EndpointOption(address string) Option {
	return func(a *Application) {
		a.endpoint = address
	}
}

// LogOutputOption uses out as a log destination.
func LogOutputOption(out io.Writer) Option {
	return func(app *Application) {
		app.logOutput = out
	}
}

// DialOption used to mock socket communication for unit testing.
func DialOption(dialer Dialer) Option {
	return func(a *Application) {
		a.dialOptions = append(a.dialOptions, grpc.WithDialer(newGRPCDialer(dialer).Dial))
	}
}

func (a *Application) setupCoreClient() error {
	var err error
	ctx, cancel := context.WithTimeout(context.Background(), a.callTimeout)
	defer cancel()
	a.conn, err = grpc.DialContext(ctx, a.endpoint, a.dialOptions...)
	if err != nil {
		return err
	}
	a.client = core.NewCoreClient(a.conn)
	return nil
}

// execute executes a task for serviceID with given data.
func (a *Application) execute(serviceID, task string, data Data) (executionID string, err error) {
	inputDataBytes, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), a.callTimeout)
	defer cancel()
	resp, err := a.client.ExecuteTask(ctx, &core.ExecuteTaskRequest{
		ServiceID: serviceID,
		TaskKey:   task,
		InputData: string(inputDataBytes),
	})
	if err != nil {
		return "", err
	}
	return resp.ExecutionID, nil
}

// startServices starts mesg services.
func (a *Application) startServices(ids ...string) error {
	idsLen := len(ids)
	if idsLen == 0 {
		return nil
	}

	errC := make(chan error, idsLen)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, id := range ids {
		go a.startService(ctx, id, errC)
	}

	for i := 0; i < idsLen; i++ {
		if err := <-errC; err != nil {
			return err
		}
	}
	return nil
}

func (a *Application) startService(ctx context.Context, id string, errC chan error) {
	_, err := a.client.StartService(ctx, &core.StartServiceRequest{
		ServiceID: id,
	})
	errC <- err
}

// Close gracefully waits current events or results to complete their process and
// and closes underlying connections.
func (a *Application) Close() error {
	a.lm.Lock()
	defer a.lm.Unlock()
	for _, listener := range a.listeners {
		listener.cancel()
	}
	for _, listener := range a.listeners {
		listener.gracefulWait.Wait()
	}
	return a.conn.Close()
}

func (a *Application) addListener(listener *Listener) {
	a.lm.Lock()
	defer a.lm.Unlock()
	a.listeners = append(a.listeners, listener)
}

func (a *Application) removeListener(listener *Listener) {
	a.lm.Lock()
	defer a.lm.Unlock()
	a.listeners = append(a.listeners, listener)
}
