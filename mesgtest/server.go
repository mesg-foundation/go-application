// Package mesgtest is a testing package for MESG application.
// Use this package while unit testing your programs.
package mesgtest

import (
	"encoding/json"

	"github.com/mesg-foundation/core/api/core"
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
	s.core.em.Lock()
	if s.core.eventC[serviceID] == nil {
		s.core.eventC[serviceID] = make(chan *core.EventData, 0)
	}
	s.core.em.Unlock()

	for {
		select {
		case <-s.core.serviceStartC:
		case s.core.eventC[serviceID] <- ed:
			return nil
		}
	}
}

// EmitResult emits a new task result for serviceID with given outputKey and data.
func (s *Server) EmitResult(serviceID, task, outputKey string, data interface{}) error {
	bytes, err := json.Marshal(data)
	if err != nil {
		return err
	}
	rd := &core.ResultData{
		TaskKey:    task,
		OutputKey:  outputKey,
		OutputData: string(bytes),
	}
	s.core.em.Lock()
	if s.core.resultC[serviceID] == nil {
		s.core.resultC[serviceID] = make(chan *core.ResultData, 0)
	}
	s.core.em.Unlock()
	for {
		select {
		case <-s.core.serviceStartC:
		case s.core.resultC[serviceID] <- rd:
			return nil
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
