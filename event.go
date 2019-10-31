package firebolt

import "time"

// Event is the struct passed through the node graph.
type Event struct {
	Payload  interface{} `json:"payload"`
	Created  time.Time   `json:"created"`
	Recovery bool        `json:"recovery"`
}

// AsyncEvent is a version of Event for nodes that support asynchronous processing.
type AsyncEvent struct {
	*Event
	ReturnError    func(error)
	ReturnEvent    func(*AsyncEvent)
	ReturnFiltered func()
}

// NewAsyncEvent creates a version of the passed Event suitable for asynchronous processing.
func NewAsyncEvent(event *Event, errFunc func(error), eventFunc func(*AsyncEvent), filterFunc func()) *AsyncEvent {
	return &AsyncEvent{
		Event:          event,
		ReturnError:    errFunc,
		ReturnEvent:    eventFunc,
		ReturnFiltered: filterFunc,
	}
}

// WithPayload returns a clone of this event with the payload replaced.
func (e *Event) WithPayload(payload interface{}) *Event {
	return &Event{
		Payload:  payload,
		Created:  e.Created,
		Recovery: e.Recovery,
	}
}

// WithPayload returns a clone of this event with the payload replaced.
func (ae *AsyncEvent) WithPayload(payload interface{}) *AsyncEvent {
	return &AsyncEvent{
		Event: &Event{
			Payload:  payload,
			Created:  ae.Created,
			Recovery: ae.Recovery,
		},
	}
}
