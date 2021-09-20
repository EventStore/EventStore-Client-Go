package event_streams

type Position struct {
	CommitPosition  uint64
	PreparePosition uint64
}

func (position Position) GreaterThan(other Position) bool {
	if position.CommitPosition > other.CommitPosition {
		return true
	}

	if position.CommitPosition == other.CommitPosition &&
		position.PreparePosition > other.PreparePosition {
		return true
	}

	return false
}

func (position Position) LessThan(other Position) bool {
	if position.CommitPosition < other.CommitPosition {
		return true
	}

	if position.CommitPosition == other.CommitPosition &&
		position.PreparePosition < other.PreparePosition {
		return true
	}

	return false
}
