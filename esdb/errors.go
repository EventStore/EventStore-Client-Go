package esdb

import (
	"fmt"
)

// ErrorCode EventStoreDB error code.
type ErrorCode int

const (
	// ErrorCodeUnknown unclassified error.
	ErrorCodeUnknown ErrorCode = iota
	// ErrorCodeUnsupportedFeature a request not supported by the targeted EventStoreDB node was sent.
	ErrorCodeUnsupportedFeature
	// ErrorCodeDeadlineExceeded a gRPC deadline exceeded error.
	ErrorCodeDeadlineExceeded
	// ErrorCodeUnauthenticated a request requires authentication and the authentication failed.
	ErrorCodeUnauthenticated
	// ErrorCodeResourceNotFound a remote resource was not found or because its access was denied.
	ErrorCodeResourceNotFound
	// ErrorCodeResourceAlreadyExists a creation request was made for a resource that already exists.
	ErrorCodeResourceAlreadyExists
	// ErrorCodeConnectionClosed when a connection is already closed.
	ErrorCodeConnectionClosed
	// ErrorCodeWrongExpectedVersion when an append request failed the optimistic concurrency on the server.
	ErrorCodeWrongExpectedVersion
	// ErrorCodeAccessDenied a request requires the right ACL.
	ErrorCodeAccessDenied
	// ErrorCodeStreamDeleted requested stream is deleted.
	ErrorCodeStreamDeleted
	// ErrorCodeParsing error when parsing data.
	ErrorCodeParsing
	// ErrorCodeInternalClient unexpected error from the client library, worthy of a GitHub issue.
	ErrorCodeInternalClient
	// ErrorCodeInternalServer unexpected error from the server, worthy of a GitHub issue.
	ErrorCodeInternalServer
	// ErrorCodeNotLeader when a request needing a leader node was executed on a follower node.
	ErrorCodeNotLeader
	// ErrorAborted when the server aborted the request.
	ErrorAborted
	// ErrorUnavailable when the EventStoreDB node became unavailable.
	ErrorUnavailable
)

// Error main client error type.
type Error struct {
	code ErrorCode
	err  error
}

// Code returns an error code.
func (e *Error) Code() ErrorCode {
	return e.code
}

// Err returns underlying error.
func (e *Error) Err() error {
	return e.err
}

// IsErrorCode checks if the error code is the same as the given one.
func (e *Error) IsErrorCode(code ErrorCode) bool {
	return e.code == code
}

func (e *Error) Error() string {
	msg := ""

	switch e.code {
	case ErrorCodeUnsupportedFeature:
		msg = "[ErrorCodeUnsupportedFeature] request not supported by the targeted EventStoreDB node"
	case ErrorCodeDeadlineExceeded:
		msg = "[ErrorCodeDeadlineExceeded] gRPC deadline exceeded error"
	case ErrorCodeUnauthenticated:
		msg = "[ErrorCodeUnauthenticated] request requires authentication and the authentication failed"
	case ErrorCodeResourceNotFound:
		msg = "[ErrorCodeResourceNotFound] a remote resource was not found or its access was denied"
	case ErrorCodeResourceAlreadyExists:
		msg = "[ErrorCodeResourceAlreadyExists] a creation request was made for a resource that already exists"
	case ErrorCodeConnectionClosed:
		msg = "[ErrorCodeConnectionClosed] the connection is already closed"
	case ErrorCodeWrongExpectedVersion:
		msg = "[ErrorCodeWrongExpectedVersion] an append request failed the optimistic concurrency on the server"
	case ErrorCodeAccessDenied:
		msg = "[ErrorCodeAccessDenied] the request requires the right ACL"
	case ErrorCodeStreamDeleted:
		msg = "[ErrorCodeStreamDeleted] requested stream is deleted"
	case ErrorCodeParsing:
		msg = "[ErrorCodeParsing] error when parsing data"
	case ErrorCodeInternalClient:
		msg = "[ErrorCodeInternalClient] unexpected error from the client library, worthy of a GitHub issue"
	case ErrorCodeInternalServer:
		msg = "[ErrorCodeInternalServer] unexpected error from the server, worthy of a GitHub issue"
	case ErrorCodeNotLeader:
		msg = "[ErrorCodeNotLeader] the request needing a leader node was executed on a follower node"
	case ErrorUnavailable:
		msg = "[ErrorUnavailable] the server is not ready to accept requests"

	default:
		msg = fmt.Sprintf("[ErrorCode %d] (sorry, this error code is not supported by the Error() method)", e.code)
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

	return &Error{code: ErrorCodeUnknown, err: err}, false
}

func unsupportedFeatureError() error {
	return &Error{code: ErrorCodeUnsupportedFeature}
}

func unknownError() error {
	return &Error{code: ErrorCodeUnknown}
}
