package event_streams

// WriteResult ...
type WriteResult struct {
	CommitPosition      uint64
	PreparePosition     uint64
	NextExpectedVersion isNextExpectedRevision
}

func (this WriteResult) GetNextVersion() (uint64, bool) {
	if nextVersion, isNextVersion := this.NextExpectedVersion.(NextExpectedVersionHasStream); isNextVersion {
		return nextVersion.Value, true
	}

	return 0, false
}

func (this WriteResult) GetNextExpectedVersionNoStream() bool {
	if _, isNoStream := this.NextExpectedVersion.(NextExpectedVersionNoStream); isNoStream {
		return true
	}

	return false
}

type isNextExpectedRevision interface {
	isNextExpectedRevision()
}

type NextExpectedVersionHasStream struct {
	Value uint64
}

func (this NextExpectedVersionHasStream) isNextExpectedRevision() {
}

type NextExpectedVersionNoStream struct{}

func (this NextExpectedVersionNoStream) isNextExpectedRevision() {
}
