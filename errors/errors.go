package errors

const (
	WrongExpectedStreamRevisionErr ErrorCode = "WrongExpectedStreamRevisionErr"
	FailedPreconditionErr          ErrorCode = "FailedPreconditionErr"
	PermissionDeniedErr            ErrorCode = "PermissionDeniedErr"
	UnauthenticatedErr             ErrorCode = "UnauthenticatedErr"
	StreamNotFoundErr              ErrorCode = "StreamNotFoundErr"
	UnknownErr                     ErrorCode = "UnknownErr"
	StreamDeletedErr               ErrorCode = "StreamDeletedErr"
	NotLeaderErr                   ErrorCode = "NotLeaderErr"
)
