package event_streams

// IsWriteStreamRevision interface is used to specify stream's expected revision
// when we want to alter a state of the stream.
// Currently, we can use WriteStreamRevision, WriteStreamRevisionNoStream,
// WriteStreamRevisionAny and WriteStreamRevisionStreamExists implementations of this interface.
type IsWriteStreamRevision interface {
	isWriteStreamRevision()
}

// WriteStreamRevision is used to specify exact (finite) expected revision of a stream.
// Set Revision field to a value you expect as stream's revision.
type WriteStreamRevision struct {
	Revision uint64
}

func (this WriteStreamRevision) isWriteStreamRevision() {}

// WriteStreamRevisionNoStream is used when stream must not exist.
// For example, if we use WriteStreamRevisionNoStream when appending events to a stream which exists
// we will receive an error WrongExpectedVersion.
type WriteStreamRevisionNoStream struct{}

func (this WriteStreamRevisionNoStream) isWriteStreamRevision() {}

// WriteStreamRevisionAny is used when we do not know if stream may or may not exist.
type WriteStreamRevisionAny struct{}

func (this WriteStreamRevisionAny) isWriteStreamRevision() {}

// WriteStreamRevisionStreamExists is used when stream must exist.
// If we try to append to a stream with WriteStreamRevisionStreamExists and stream does not exist we will
// receive WrongExpectedVersion errors.
type WriteStreamRevisionStreamExists struct{}

func (this WriteStreamRevisionStreamExists) isWriteStreamRevision() {}

// IsReadStreamRevision interface is used when we want to read events from a stream.
// Currently, we can use ReadStreamRevision, ReadStreamRevisionStart and ReadStreamRevisionEnd.
type IsReadStreamRevision interface {
	isReadStreamRevision()
}

// ReadStreamRevision is used to indicate an exact revision from which we want to start reading
type ReadStreamRevision struct {
	Revision uint64
}

func (this ReadStreamRevision) isReadStreamRevision() {}

// ReadStreamRevisionStart is used when we want to start to read events from beginning of a stream.
type ReadStreamRevisionStart struct{}

func (this ReadStreamRevisionStart) isReadStreamRevision() {
}

// ReadStreamRevisionEnd is used when we want to start to read events from the end of a stream.
type ReadStreamRevisionEnd struct{}

func (this ReadStreamRevisionEnd) isReadStreamRevision() {
}

// IsReadPositionAll interface is used when we want to specify starting read position when we want to read
// events form $all stream.
// Currently, we can use ReadPositionAll, ReadPositionAllStart and ReadPositionAllEnd.
type IsReadPositionAll interface {
	isReadPositionAll()
}

// ReadPositionAll is used when we want to specify exact position from which we want to start to read events
// from $all stream.
// Each event written to EventStore has a commit and prepare position.
type ReadPositionAll struct {
	CommitPosition  uint64
	PreparePosition uint64
}

func (this ReadPositionAll) isReadPositionAll() {}

// ReadPositionAllStart is used when we want to start to read events from the beginning of $all stream.
type ReadPositionAllStart struct{}

func (this ReadPositionAllStart) isReadPositionAll() {}

// ReadPositionAllEnd is used when we want to start to read events from the end of $all stream.
type ReadPositionAllEnd struct{}

func (this ReadPositionAllEnd) isReadPositionAll() {}
