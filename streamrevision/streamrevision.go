package streamrevision

// StreamRevision ...
type StreamRevision uint64

const (
	// StreamRevisionStreamExists ...
	StreamRevisionStreamExists = StreamRevision(StreamRevisionEnd - 1)
	// StreamRevisionAny ...
	StreamRevisionAny = StreamRevision(StreamRevisionEnd - 2)
	// StreamRevisionNoStream ...
	StreamRevisionNoStream = StreamRevision(StreamRevisionEnd - 3)
)

func NewStreamRevision(value uint64) StreamRevision {
	return StreamRevision(value)
}

// StreamRevisionStart ...
const StreamRevisionStart uint64 = 0

// StreamRevisionEnd ...
const StreamRevisionEnd uint64 = ^uint64(0)
