package errors

const (
	WrongExpectedStreamRevisionErr   ErrorCode = "WrongExpectedStreamRevisionErr"
	FailedPreconditionErr            ErrorCode = "FailedPreconditionErr"
	PermissionDeniedErr              ErrorCode = "PermissionDeniedErr"
	DeadlineExceededErr              ErrorCode = "DeadlineExceededErr"
	CanceledErr                      ErrorCode = "CanceledErr"
	UnauthenticatedErr               ErrorCode = "UnauthenticatedErr"
	StreamNotFoundErr                ErrorCode = "StreamNotFoundErr"
	UnknownErr                       ErrorCode = "UnknownErr"
	StreamDeletedErr                 ErrorCode = "StreamDeletedErr"
	NotLeaderErr                     ErrorCode = "NotLeaderErr"
	MaximumAppendSizeExceededErr     ErrorCode = "MaximumAppendSizeExceededErr"
	UserNotFoundErr                  ErrorCode = "UserNotFoundErr"
	EndOfStream                      ErrorCode = "EndOfStream"
	FatalError                       ErrorCode = "FatalError"
	MaximumSubscriberCountReached    ErrorCode = "MaximumSubscriberCountReached"
	PersistentSubscriptionDroppedErr ErrorCode = "PersistentSubscriptionDroppedErr"
	ScavengeNotFoundErr              ErrorCode = "ScavengeNotFoundErr"
	InvalidArgumentErr               ErrorCode = "InvalidArgumentErr"
)
