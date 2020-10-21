package errors

import (
	"errors"
)

// ErrWrongExpectedStreamRevision ...
var ErrWrongExpectedStreamRevision = errors.New("WrongExceptedStreamRevision")

// ErrPermissionDenied ...
var ErrPermissionDenied = errors.New("PermissionDenied")

// ErrUnAuthenticated
var ErrUnauthenticated = errors.New("Unauthenticated")

// ErrStreamNotFound is returned when a read requests gets a stream not found response
// from the EventStore.
// Example usage:
// ```go
// events, err := client.ReadStreamEvents(...)
// if err == errors.ErrStreamNotFound {
//   // handle the stream not being found
// }
// ```
var ErrStreamNotFound = errors.New("Failed to perform read because the stream was not found")
