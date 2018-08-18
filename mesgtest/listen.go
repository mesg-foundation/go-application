package mesgtest

// EventListen holds information about an event listen request.
type EventListen struct {
	serviceID string
	eventKey  string
}

// ServiceID returns the id of service that events are emitted from.
func (l *EventListen) ServiceID() string {
	return l.serviceID
}

// EventFilter returns the event name.
func (l *EventListen) EventFilter() string {
	return l.eventKey
}

// ResultListen holds information about a result listen request.
type ResultListen struct {
	serviceID     string
	outputKey     string
	taskKey       string
	executionTags []string
}

// ServiceID returns the id of service that results are emitted from.
func (l *ResultListen) ServiceID() string {
	return l.serviceID
}

// KeyFilter returns the output key name.
func (l *ResultListen) KeyFilter() string {
	return l.outputKey
}

// TaskFilter returns the task key name.
func (l *ResultListen) TaskFilter() string {
	return l.taskKey
}

// Tags returns the execution tags filter.
func (l *ResultListen) Tags() []string {
	return l.executionTags
}
