// Package mesgtest is a testing package for MESG application.
// Use this package while unit testing your programs.
package mesgtest

import (
	"encoding/json"

	core "github.com/mesg-foundation/go-application/proto"
)

// Server is a test server.
type Server struct {
	core   *coreServer
	socket *Socket
}

// NewServer creates a new test server.
func NewServer() *Server {
	return &Server{
		core:   newCoreServer(),
		socket: newSocket(),
	}
}

// Start starts the test server.
func (s *Server) Start() error {
	return s.socket.listen(s.core)
}

// Close closes test server.
func (s *Server) Close() error {
	return s.socket.close()
}

// Socket returns a in-memory socket for client application.
func (s *Server) Socket() *Socket {
	return s.socket
}

// LastServiceStart returns the chan that receives last service start request's info.
func (s *Server) LastServiceStart() <-chan *ServiceStart {
	return s.core.serviceStartC
}

// LastEventListen returns the chan that receives last event listen request's info.
func (s *Server) LastEventListen() <-chan *EventListen {
	s.flushServiceStarts()
	return s.core.listenEventC
}

// LastResultListen returns the chan that receives last result listen request's info.
func (s *Server) LastResultListen() <-chan *ResultListen {
	s.flushServiceStarts()
	return s.core.listenResultC
}

// LastExecute returns the chan that receives last task execution's info.
func (s *Server) LastExecute() <-chan *Execute {
	s.flushServiceStarts()
	return s.core.executeC
}

// EmitEvent emits a new event for serviceID with given data.
func (s *Server) EmitEvent(serviceID, event string, data interface{}) error {
	bytes, err := json.Marshal(data)
	if err != nil {
		return err
	}
	ed := &core.EventData{
		EventKey:  event,
		EventData: string(bytes),
	}
	s.core.initEvent(serviceID)
	serviceEvent := s.core.event[serviceID]
	for {
		select {
		case <-s.core.serviceStartC:
		case serviceEvent.dataC <- ed:
			return <-serviceEvent.doneC
		}
	}
}

// EmitResult emits a new task result for serviceID with given outputKey and data.
func (s *Server) EmitResult(serviceID, task, outputKey string, data interface{}, executionTags []string) error {
	bytes, err := json.Marshal(data)
	if err != nil {
		return err
	}
	rd := &core.ResultData{
		TaskKey:       task,
		OutputKey:     outputKey,
		OutputData:    string(bytes),
		ExecutionTags: executionTags,
	}
	s.core.initResult(serviceID)
	result := s.core.result[serviceID]
	for {
		select {
		case <-s.core.serviceStartC:
		case result.dataC <- rd:
			return <-result.doneC
		}
	}
}

// MarkServiceAsNonExistent marks a service id as non-exists server.
func (s *Server) MarkServiceAsNonExistent(id string) {
	s.core.nonExistentServices = append(s.core.nonExistentServices, id)
}

func (s *Server) flushServiceStarts() {
	for {
		select {
		case <-s.core.serviceStartC:
		default:
			return
		}
	}
}
