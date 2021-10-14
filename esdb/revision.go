package esdb

type Any struct{}
type StreamExists struct{}
type NoStream struct{}

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
