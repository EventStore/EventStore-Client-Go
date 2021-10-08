package event_streams

import "github.com/pivonroll/EventStore-Client-Go/errors"

const WrongExpectedVersionErr errors.ErrorCode = "WrongExpectedVersionErr"

func newWrongExpectedVersionError() WrongExpectedVersion {
	return WrongExpectedVersion{
		err: errors.NewErrorCode(WrongExpectedVersionErr),
	}
}

// WrongExpectedVersion is an error returned when writing events to a stream fails gracefully.
// That means that if grpc connection is broken, WrongExpectedVersion error will not be returned, but a more
// general error of type errors.Error will be returned.
// Writing of events can fail gracefully if client sends a wrong expected revision.
type WrongExpectedVersion struct {
	err errors.Error
	// Types that are assignable to currentRevision2060:
	//	appendResponseWrongCurrentRevision2060
	//	appendResponseWrongCurrentRevisionNoStream2060
	currentRevision2060 isAppendResponseWrongCurrentRevision2060
	// Types that are assignable to expectedRevision2060:
	//	appendResponseWrongExpectedRevision2060
	//	appendResponseWrongExpectedRevisionAny2060
	//	appendResponseWrongExpectedRevisionStreamExists2060
	expectedRevision2060 isAppendResponseWrongExpectedRevision2060
	// Types that are assignable to currentRevision:
	//	appendResponseWrongCurrentRevision
	//	appendResponseWrongCurrentRevisionNoStream
	currentRevision isAppendResponseWrongCurrentRevision
	// Types that are assignable to expectedRevision:
	//	appendResponseWrongExpectedRevision
	//	appendResponseWrongExpectedRevisionAny
	//	appendResponseWrongExpectedRevisionStreamExists
	//	appendResponseWrongExpectedRevisionNoStream
	expectedRevision isAppendResponseWrongExpectedRevision
}

// Error returns a string representation of an error
func (exception WrongExpectedVersion) Error() string {
	return exception.err.Error()
}

// Code returns a code of an error received.
// Currently, only WrongExpectedVersionErr code will be returned.
func (exception WrongExpectedVersion) Code() errors.ErrorCode {
	return exception.err.Code()
}

// GetExpectedRevision returns a finite expected revision of a stream which
// client has sent when it wanted to open a protobuf stream to write events.
// It panics if client did not send a finite expected revision. Use IsExpectedRevisionFinite to check
// if expected revision is finite.
func (exception WrongExpectedVersion) GetExpectedRevision() uint64 {
	if revision, ok := exception.expectedRevision.(appendResponseWrongExpectedRevision); ok {
		return revision.expectedRevision
	} else if revision, ok := exception.expectedRevision2060.(appendResponseWrongExpectedRevision2060); ok {
		return revision.expectedRevision
	}

	panic("Not an expected revision")
}

// IsExpectedRevisionFinite returns true if client has sent a finite expected revision of a stream.
func (exception WrongExpectedVersion) IsExpectedRevisionFinite() bool {
	if _, ok := exception.expectedRevision.(appendResponseWrongExpectedRevision); ok {
		return true
	} else if _, ok := exception.expectedRevision2060.(appendResponseWrongExpectedRevision2060); ok {
		return true
	}

	return false
}

// IsExpectedRevisionNoStream returns true if client has sent WriteStreamRevisionNoStream when it wanted to
// open a protobuf stream to write events to EventStoreDB's event stream.
func (exception WrongExpectedVersion) IsExpectedRevisionNoStream() bool {
	_, ok := exception.expectedRevision.(appendResponseWrongExpectedRevisionNoStream)
	return ok
}

// IsExpectedRevisionAny returns true if client has sent WriteStreamRevisionAny when it wanted to
// open a protobuf stream to write events to EventStoreDB's event stream.
func (exception WrongExpectedVersion) IsExpectedRevisionAny() bool {
	if _, ok := exception.expectedRevision.(appendResponseWrongExpectedRevisionAny); ok {
		return true
	} else if _, ok := exception.expectedRevision2060.(appendResponseWrongExpectedRevisionAny2060); ok {
		return true
	}

	return false
}

// IsExpectedRevisionStreamExists returns true if client has sent WriteStreamRevisionStreamExists
// when it wanted to open a protobuf stream to write events to EventStoreDB's event stream.
func (exception WrongExpectedVersion) IsExpectedRevisionStreamExists() bool {
	if _, ok := exception.expectedRevision.(appendResponseWrongExpectedRevisionStreamExists); ok {
		return true
	} else if _, ok := exception.expectedRevision2060.(appendResponseWrongExpectedRevisionStreamExists2060); ok {
		return true
	}

	return false
}

// IsCurrentRevisionNoStream returns true if client has sent WriteStreamRevisionNoStream
// when it wanted to open a protobuf stream to write events to EventStoreDB's event stream.
func (exception WrongExpectedVersion) IsCurrentRevisionNoStream() bool {
	if _, ok := exception.currentRevision.(appendResponseWrongCurrentRevisionNoStream); ok {
		return true
	} else if _, ok := exception.currentRevision2060.(appendResponseWrongCurrentRevisionNoStream2060); ok {
		return true
	}

	return false
}

// GetCurrentRevision returns an actual current revision of a stream and a true value if current revision
// was received.
// If no current revision was received a zero is returned together with a false value.
func (exception WrongExpectedVersion) GetCurrentRevision() (uint64, bool) {
	if revision, ok := exception.currentRevision.(appendResponseWrongCurrentRevision); ok {
		return revision.currentRevision, true
	} else if revision, ok := exception.currentRevision2060.(appendResponseWrongCurrentRevision2060); ok {
		return revision.currentRevision, true
	}

	return 0, false
}
