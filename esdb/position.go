package esdb

// StreamRevision returns a stream position at a specific event revision.
type StreamRevision struct {
	Value uint64
}

// Revision returns a stream position at a specific event revision.
func Revision(value uint64) StreamRevision {
	return StreamRevision{
		Value: value,
	}
}

// Start represents the beginning of a stream or $all.
type Start struct {
}

// End represents the end of a stream or $all.
type End struct {
}

// StreamPosition represents a logical position in a stream.
type StreamPosition interface {
	isStreamPosition()
}

// AllPosition represents a logical position in the $all stream.
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
