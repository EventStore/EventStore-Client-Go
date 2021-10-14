package esdb

type StreamRevision struct {
	Value uint64
}

func Revision(value uint64) StreamRevision {
	return StreamRevision{
		Value: value,
	}
}

type Start struct {
}

type End struct {
}

type StreamPosition interface {
	isStreamPosition()
}

type AllPosition interface {
	isAllPosition()
}

func (r StreamRevision) isStreamPosition() {
}

func (r Position) isAllPosition() {
}

func (r Start) isStreamPosition() {
}

func (r End) isStreamPosition() {
}

func (r Start) isAllPosition() {
}

func (r End) isAllPosition() {
}
