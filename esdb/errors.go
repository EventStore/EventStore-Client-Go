package esdb

import (
	"errors"
	"fmt"
)

// ErrWrongExpectedStreamRevision ...
var ErrWrongExpectedStreamRevision = errors.New("WrongExpectedStreamRevision")

// ErrPermissionDenied ...
var ErrPermissionDenied = errors.New("PermissionDenied")

// ErrStreamNotFound is returned when a read requests gets a stream not found response
// from the EventStore.
// Example usage:
// ```go
// events, err := esdb.ReadStream(...)
// if err == errors.ErrStreamNotFound {
//   // handle the stream not being found
// }
// ```
var ErrStreamNotFound = errors.New("Failed to perform read because the stream was not found")

type StreamDeletedError struct {
	StreamName string
}

func (e *StreamDeletedError) Error() string {
	return fmt.Sprintf("stream '%s' is deleted", e.StreamName)
}

var ErrResourceNotFound = errors.New("ResourceNotFound")

type ServerError struct {
	Code int
	Err  error
}

func (e *ServerError) Error() string {
	return fmt.Sprintf("Server error: %d: %s", e.Code, e.Err)
}

func (err ServerError) Unwrap() error {
	return err.Err
}

type InternalClientError struct {
	Code int
	Err  error
}

func (e *InternalClientError) Error() string {
	return fmt.Sprintf("Internal client error: code %d: %s", e.Code, e.Err)
}

func (err InternalClientError) Unwrap() error {
	return err.Err
}
