package mesgtest

import (
	"context"
	"errors"
	"sync"

	"github.com/mesg-foundation/core/api/core"
	uuid "github.com/satori/go.uuid"
	"google.golang.org/grpc"
)

// coreServer implements MESG's core/application server.
type coreServer struct {
	serviceStartC chan *ServiceStart
	listenEventC  chan *EventListen
	listenResultC chan *ResultListen

	executeC chan *Execute

	eventC  map[string]chan *core.EventData
	resultC map[string]chan *core.ResultData
	em      sync.Mutex

	nonExistentServices []string
}

func newCoreServer() *coreServer {
	return &coreServer{
		serviceStartC: make(chan *ServiceStart, 2),
		listenEventC:  make(chan *EventListen, 1),
		listenResultC: make(chan *ResultListen, 1),
		executeC:      make(chan *Execute, 1),
		eventC:        make(map[string]chan *core.EventData, 0),
		resultC:       make(map[string]chan *core.ResultData, 0),
	}
}

func (s *coreServer) DeleteService(ctx context.Context,
	request *core.DeleteServiceRequest) (reply *core.DeleteServiceReply, err error) {
	return &core.DeleteServiceReply{}, nil
}

func (s *coreServer) DeployService(ctx context.Context,
	request *core.DeployServiceRequest) (reply *core.DeployServiceReply, err error) {
	return &core.DeployServiceReply{}, nil
}

func (s *coreServer) ExecuteTask(ctx context.Context,
	request *core.ExecuteTaskRequest) (reply *core.ExecuteTaskReply, err error) {
	s.executeC <- &Execute{
		serviceID: request.ServiceID,
		task:      request.TaskKey,
		data:      request.InputData,
	}
	uuidV4, err := uuid.NewV4()
	id := uuidV4.String()
	return &core.ExecuteTaskReply{
		ExecutionID: id,
	}, err
}

func (s *coreServer) GetService(ctx context.Context,
	request *core.GetServiceRequest) (reply *core.GetServiceReply, err error) {
	return &core.GetServiceReply{}, nil
}

func (s *coreServer) ListServices(ctx context.Context,
	request *core.ListServicesRequest) (reply *core.ListServicesReply, err error) {
	return &core.ListServicesReply{}, nil
}

func (s *coreServer) ListenEvent(request *core.ListenEventRequest,
	stream core.Core_ListenEventServer) (err error) {
	s.listenEventC <- &EventListen{
		serviceID: request.ServiceID,
		event:     request.EventFilter,
	}
	s.em.Lock()
	if s.eventC[request.ServiceID] == nil {
		s.eventC[request.ServiceID] = make(chan *core.EventData, 0)
	}
	s.em.Unlock()

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case event := <-s.eventC[request.ServiceID]:
			if err := stream.Send(event); err != nil {
				return err
			}
		}
	}
}

func (s *coreServer) ListenResult(request *core.ListenResultRequest,
	stream core.Core_ListenResultServer) (err error) {
	s.listenResultC <- &ResultListen{
		serviceID: request.ServiceID,
		key:       request.OutputFilter,
		task:      request.TaskFilter,
	}

	s.em.Lock()
	if s.resultC[request.ServiceID] == nil {
		s.resultC[request.ServiceID] = make(chan *core.ResultData, 0)
	}
	s.em.Unlock()

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case result := <-s.resultC[request.ServiceID]:
			if err := stream.Send(result); err != nil {
				return err
			}
		}
	}
}

var ErrServiceDoesNotExists = errors.New("service does not exists")

func (s *coreServer) StartService(ctx context.Context,
	request *core.StartServiceRequest) (reply *core.StartServiceReply, err error) {
	for _, id := range s.nonExistentServices {
		if request.ServiceID == id {
			return &core.StartServiceReply{}, ErrServiceDoesNotExists
		}
	}
	s.serviceStartC <- &ServiceStart{
		serviceID: request.ServiceID,
	}
	return &core.StartServiceReply{}, nil
}

func (s *coreServer) StopService(ctx context.Context,
	request *core.StopServiceRequest) (reply *core.StopServiceReply, err error) {
	return &core.StopServiceReply{}, nil
}

type eventDataStream struct {
	eventC chan *core.EventData
	ctx    context.Context
	cancel context.CancelFunc
	grpc.ServerStream
}

func newEventDataStream() *eventDataStream {
	ctx, cancel := context.WithCancel(context.Background())
	return &eventDataStream{
		eventC: make(chan *core.EventData, 0),
		ctx:    ctx,
		cancel: cancel,
	}
}

func (s eventDataStream) Send(data *core.EventData) error {
	s.eventC <- data
	return nil
}

func (s eventDataStream) Context() context.Context {
	return s.ctx
}

func (s eventDataStream) close() {
	s.cancel()
}

type resultDataStream struct {
	resultC chan *core.ResultData
	ctx     context.Context
	cancel  context.CancelFunc
	grpc.ServerStream
}

func newResultDataStream() *resultDataStream {
	ctx, cancel := context.WithCancel(context.Background())
	return &resultDataStream{
		resultC: make(chan *core.ResultData, 0),
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (s resultDataStream) Send(data *core.ResultData) error {
	s.resultC <- data
	return nil
}

func (s resultDataStream) Context() context.Context {
	return s.ctx
}

func (s resultDataStream) close() {
	s.cancel()
}
