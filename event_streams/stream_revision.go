package event_streams

type IsWriteStreamRevision interface {
	isWriteStreamRevision()
}

type WriteStreamRevision struct {
	Revision uint64
}

func (this WriteStreamRevision) isWriteStreamRevision() {}

type WriteStreamRevisionNoStream struct{}

func (this WriteStreamRevisionNoStream) isWriteStreamRevision() {}

type WriteStreamRevisionAny struct{}

func (this WriteStreamRevisionAny) isWriteStreamRevision() {}

type WriteStreamRevisionStreamExists struct{}

func (this WriteStreamRevisionStreamExists) isWriteStreamRevision() {}

type IsReadStreamRevision interface {
	isReadStreamRevision()
}

type ReadStreamRevision struct {
	Revision uint64
}

func (this ReadStreamRevision) isReadStreamRevision() {}

type ReadStreamRevisionStart struct{}

func (this ReadStreamRevisionStart) isReadStreamRevision() {
}

type ReadStreamRevisionEnd struct{}

func (this ReadStreamRevisionEnd) isReadStreamRevision() {
}

type IsReadPositionAll interface {
	isReadPositionAll()
}

type ReadPositionAll struct {
	CommitPosition  uint64
	PreparePosition uint64
}

func (this ReadPositionAll) isReadPositionAll() {}

type ReadPositionAllStart struct{}

func (this ReadPositionAllStart) isReadPositionAll() {}

type ReadPositionAllEnd struct{}

func (this ReadPositionAllEnd) isReadPositionAll() {}
