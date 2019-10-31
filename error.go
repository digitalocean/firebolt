package firebolt

import (
	"encoding/json"
	"fmt"
	"time"
)

// EventError is the structure passed to any `error_handler`.   When an error occurs in a node that has an error_handler configured,
// firebolt wraps up the error and the causing event in an EventError.   This EventError needs to be handled in your error_handler's
// Process() method.
type EventError struct {
	Timestamp time.Time   `json:"timestamp"`
	Event     interface{} `json:"event"`
	Err       error       `json:"error"`
}

// NewEventError constructs
func NewEventError(event *Event, err error) EventError {
	return EventError{
		Timestamp: event.Created,
		Event:     event.Payload,
		Err:       err,
	}
}

// MarshalJSON converts an EventError to json, replacing any err  values that are not FBError with a FBError wrapper
// so that the generated JSON has consistent structure.
func (ee EventError) MarshalJSON() ([]byte, error) {
	eventErr := ee.Err
	if _, ok := ee.Err.(FBError); ok {
		// the error is a type that knows how to JSON marshal itself
	} else {
		// plain vanilla errors should be converted to FBErrors
		eventErr = NewFBError("ERR_UNKNOWN", ee.Err.Error())
	}

	return json.Marshal(struct {
		Timestamp time.Time   `json:"timestamp"`
		Event     interface{} `json:"event"`
		Err       error       `json:"error"`
	}{
		Timestamp: ee.Timestamp,
		Event:     ee.Event,
		Err:       eventErr,
	})
}

// FBError is an optional error type that can be returned from the Process() method in your nodes,
type FBError struct {
	Code      string      `json:"code"`
	Msg       string      `json:"message"`
	ErrorInfo interface{} `json:"errorinfo,omitempty"`
}

// FBErrorOpt is an option that allows you do add optional data to an FBError when calling the constructor.
type FBErrorOpt func(FBError) FBError

// WithInfo adds information to FBError.ErrorInfo.
func WithInfo(info interface{}) FBErrorOpt {
	return func(e FBError) FBError {
		e.ErrorInfo = info
		return e
	}
}

// NewFBError constructs a firebolt error.   Returning a FBError (rather than just 'error') from the Process() method
// in your nodes will result in more structured error reports if you use `error_handler` nodes.
func NewFBError(code string, msg string, opts ...FBErrorOpt) FBError {
	e := FBError{Code: code, Msg: msg}
	for _, opt := range opts {
		e = opt(e)
	}
	return e
}

func (f FBError) Error() string {
	return fmt.Sprintf("%s: %s", f.Code, f.Msg)
}
