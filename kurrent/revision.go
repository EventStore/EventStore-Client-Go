package kurrent

// Any means the write should not conflict with anything and should always succeed.
type Any struct{}

// StreamExists means the stream should exist.
type StreamExists struct{}

// NoStream means the stream being written to should not yet exist.
type NoStream struct{}

// ExpectedRevision the use of expected revision can be a bit tricky especially when discussing guaranties given by
// KurrentDB server. The KurrentDB server will assure idempotency for all requests using any value in
// ExpectedRevision except Any. When using Any, the KurrentDB server will do its best to assure idempotency but
// will not guarantee it.
type ExpectedRevision interface {
	isExpectedRevision()
}

func (r Any) isExpectedRevision() {
}

func (r StreamExists) isExpectedRevision() {
}

func (r NoStream) isExpectedRevision() {
}

func (r StreamRevision) isExpectedRevision() {
}
