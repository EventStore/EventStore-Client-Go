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
