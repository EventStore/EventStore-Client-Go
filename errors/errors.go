package errors

const (
	WrongExpectedStreamRevisionErr ErrorCode = "WrongExpectedStreamRevisionErr"
	FailedPreconditionErr          ErrorCode = "FailedPreconditionErr"
	PermissionDeniedErr            ErrorCode = "PermissionDeniedErr"
	DeadlineExceededErr            ErrorCode = "DeadlineExceededErr"
	UnauthenticatedErr             ErrorCode = "UnauthenticatedErr"
	StreamNotFoundErr              ErrorCode = "StreamNotFoundErr"
	UnknownErr                     ErrorCode = "UnknownErr"
	StreamDeletedErr               ErrorCode = "StreamDeletedErr"
	NotLeaderErr                   ErrorCode = "NotLeaderErr"
	MaximumAppendSizeExceededErr   ErrorCode = "MaximumAppendSizeExceededErr"
)
