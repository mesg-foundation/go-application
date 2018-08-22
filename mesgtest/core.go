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

	event  map[string]*event
	result map[string]*result
	em     sync.Mutex

	nonExistentServices []string

	// make compiler happy for unimplemented methods
	core.CoreServer
}

func newCoreServer() *coreServer {
	return &coreServer{
		serviceStartC: make(chan *ServiceStart, 2),
		listenEventC:  make(chan *EventListen, 1),
		listenResultC: make(chan *ResultListen, 1),
		executeC:      make(chan *Execute, 1),
		event:         make(map[string]*event, 0),
		result:        make(map[string]*result, 0),
	}
}

type result struct {
	dataC chan *core.ResultData
	doneC chan error
}

func newResult() *result {
	return &result{
		dataC: make(chan *core.ResultData, 0),
		doneC: make(chan error, 0),
	}
}

func (s *coreServer) initResult(serviceID string) {
	s.em.Lock()
	defer s.em.Unlock()
	if s.result[serviceID] == nil {
		s.result[serviceID] = newResult()
	}
}

type event struct {
	dataC chan *core.EventData
	doneC chan error
}

func newEvent() *event {
	return &event{
		dataC: make(chan *core.EventData, 0),
		doneC: make(chan error, 0),
	}
}

func (s *coreServer) initEvent(serviceID string) {
	s.em.Lock()
	defer s.em.Unlock()
	if s.event[serviceID] == nil {
		s.event[serviceID] = newEvent()
	}
}

func (s *coreServer) ExecuteTask(ctx context.Context,
	request *core.ExecuteTaskRequest) (reply *core.ExecuteTaskReply, err error) {
	s.executeC <- &Execute{
		serviceID:     request.ServiceID,
		task:          request.TaskKey,
		data:          request.InputData,
		executionTags: request.ExecutionTags,
	}
	uuidV4, err := uuid.NewV4()
	id := uuidV4.String()
	return &core.ExecuteTaskReply{
		ExecutionID: id,
	}, err
}

func (s *coreServer) ListenEvent(request *core.ListenEventRequest,
	stream core.Core_ListenEventServer) (err error) {
	s.listenEventC <- &EventListen{
		serviceID: request.ServiceID,
		eventKey:  request.EventFilter,
	}
	s.initEvent(request.ServiceID)
	event := s.event[request.ServiceID]
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case data := <-event.dataC:
			if err := stream.Send(data); err != nil {
				event.doneC <- err
				return err
			}
			event.doneC <- nil
		}
	}
}

func (s *coreServer) ListenResult(request *core.ListenResultRequest,
	stream core.Core_ListenResultServer) (err error) {
	s.listenResultC <- &ResultListen{
		serviceID:     request.ServiceID,
		outputKey:     request.OutputFilter,
		taskKey:       request.TaskFilter,
		executionTags: request.TagFilters,
	}
	s.initResult(request.ServiceID)
	result := s.result[request.ServiceID]
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case data := <-result.dataC:
			if err := stream.Send(data); err != nil {
				result.doneC <- err
				return err
			}
			result.doneC <- nil
		}
	}
}

// ErrServiceDoesNotExists returned when a service is not exists during service start.
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
