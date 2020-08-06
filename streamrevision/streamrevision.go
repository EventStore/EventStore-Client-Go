package streamrevision

// StreamRevision ...
type StreamRevision uint64

const (
	// StreamRevisionNoStream ...
	StreamRevisionNoStream StreamRevision = iota
	// StreamRevisionAny ...
	StreamRevisionAny
	// StreamRevisionStreamExists ...
	StreamRevisionStreamExists
)

func NewStreamRevision(value uint64) StreamRevision {
	return StreamRevision(value)
}

// StreamRevisionStart ...
const StreamRevisionStart uint64 = 0

// StreamRevisionEnd ...
const StreamRevisionEnd uint64 = ^uint64(0)
