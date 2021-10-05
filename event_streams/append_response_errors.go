package event_streams

import "github.com/pivonroll/EventStore-Client-Go/errors"

const WrongExpectedVersionErr errors.ErrorCode = "WrongExpectedVersionErr"

func newWrongExpectedVersionError() WrongExpectedVersion {
	return WrongExpectedVersion{
		err: errors.NewErrorCode(WrongExpectedVersionErr),
	}
}

type WrongExpectedVersion struct {
	err errors.Error
	// Types that are assignable to CurrentRevisionOption_20_6_0:
	//	AppendResponseWrongCurrentRevision2060
	//	AppendResponseWrongCurrentRevisionNoStream2060
	CurrentRevision_20_6_0 isAppendResponseWrongCurrentRevision2060
	// Types that are assignable to ExpectedRevisionOption_20_6_0:
	//	AppendResponseWrongExpectedRevision2060
	//	AppendResponseWrongExpectedRevisionAny2060
	//	AppendResponseWrongExpectedRevisionStreamExists2060
	ExpectedRevision_20_6_0 isAppendResponseWrongExpectedRevision2060
	// Types that are assignable to CurrentRevisionOption:
	//	AppendResponseWrongCurrentRevision
	//	AppendResponseWrongCurrentRevisionNoStream
	CurrentRevision isAppendResponseWrongCurrentRevision
	// Types that are assignable to ExpectedRevisionOption:
	//	AppendResponseWrongExpectedRevision
	//	AppendResponseWrongExpectedRevisionAny
	//	AppendResponseWrongExpectedRevisionStreamExists
	//	AppendResponseWrongExpectedRevisionNoStream
	ExpectedRevision isAppendResponseWrongExpectedRevision
}

func (exception WrongExpectedVersion) Error() string {
	return exception.err.Error()
}

func (exception WrongExpectedVersion) Code() errors.ErrorCode {
	return exception.err.Code()
}

func (exception WrongExpectedVersion) GetExpectedRevision() uint64 {
	if revision, ok := exception.ExpectedRevision.(AppendResponseWrongExpectedRevision); ok {
		return revision.ExpectedRevision
	} else if revision, ok := exception.ExpectedRevision_20_6_0.(AppendResponseWrongExpectedRevision2060); ok {
		return revision.ExpectedRevision
	}

	panic("Not an expected revision")
}

func (exception WrongExpectedVersion) IsExpectedRevisionFinite() bool {
	if _, ok := exception.ExpectedRevision.(AppendResponseWrongExpectedRevision); ok {
		return true
	} else if _, ok := exception.ExpectedRevision_20_6_0.(AppendResponseWrongExpectedRevision2060); ok {
		return true
	}

	return false
}

func (exception WrongExpectedVersion) IsExpectedRevisionNoStream() bool {
	_, ok := exception.ExpectedRevision.(AppendResponseWrongExpectedRevisionNoStream)
	return ok
}

func (exception WrongExpectedVersion) IsExpectedRevisionAny() bool {
	if _, ok := exception.ExpectedRevision.(AppendResponseWrongExpectedRevisionAny); ok {
		return true
	} else if _, ok := exception.ExpectedRevision_20_6_0.(AppendResponseWrongExpectedRevisionAny2060); ok {
		return true
	}

	return false
}

func (exception WrongExpectedVersion) IsExpectedRevisionStreamExists() bool {
	if _, ok := exception.ExpectedRevision.(AppendResponseWrongExpectedRevisionStreamExists); ok {
		return true
	} else if _, ok := exception.ExpectedRevision_20_6_0.(AppendResponseWrongExpectedRevisionStreamExists2060); ok {
		return true
	}

	return false
}

func (exception WrongExpectedVersion) IsCurrentRevisionNoStream() bool {
	if _, ok := exception.CurrentRevision.(AppendResponseWrongCurrentRevisionNoStream); ok {
		return true
	} else if _, ok := exception.CurrentRevision_20_6_0.(AppendResponseWrongCurrentRevisionNoStream2060); ok {
		return true
	}

	return false
}

func (exception WrongExpectedVersion) GetCurrentRevision() (uint64, bool) {
	if revision, ok := exception.CurrentRevision.(AppendResponseWrongCurrentRevision); ok {
		return revision.CurrentRevision, true
	} else if revision, ok := exception.CurrentRevision_20_6_0.(AppendResponseWrongCurrentRevision2060); ok {
		return revision.CurrentRevision, true
	}

	return 0, false
}
