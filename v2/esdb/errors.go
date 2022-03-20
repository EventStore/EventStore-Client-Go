package esdb

import (
	"fmt"
)

type ErrorCode int

const (
	ErrorUnknown ErrorCode = iota
	ErrorUnsupportedFeature
	ErrorDeadlineExceeded
	ErrorUnauthenticated
	ErrorResourceNotFound
	ErrorResourceAlreadyExists
	ErrorConnectionClosed
	ErrorWrongExpectedVersion
	ErrorAccessDenied
	ErrorStreamDeleted
	ErrorParsing
	ErrorInternalClient
	ErrorInternalServer
	ErrorNotLeader
)

type Error struct {
	code ErrorCode
	err  error
}

func (e *Error) Code() ErrorCode {
	return e.code
}

func (e *Error) Err() error {
	return e.err
}

func (e *Error) Error() string {
	msg := ""

	switch e.code {
	case ErrorUnsupportedFeature:
		msg = "unsupported feature"
	}

	if e.err != nil {
		msg = fmt.Sprintf("%s: %v", msg, e.Err())
	}

	return msg
}

func (e *Error) Unwrap() error {
	return e.Err()
}

func FromError(err error) (*Error, bool) {
	if err == nil {
		return nil, true
	}

	if esErr, ok := err.(*Error); ok {
		return esErr, false
	}

	return &Error{code: ErrorUnknown, err: err}, false
}

func unsupportedFeatureError() error {
	return &Error{code: ErrorUnsupportedFeature}
}

func unknownError() error {
	return &Error{code: ErrorUnknown}
}
