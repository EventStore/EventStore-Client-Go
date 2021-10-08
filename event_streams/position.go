package event_streams

// Position represents event's position in stream $all.
type Position struct {
	CommitPosition  uint64
	PreparePosition uint64
}

// GreaterThan returns true if receiver's position is greater than provided position.
// Position is greater if commit position is greater than argument's commit position or
// if commit position is the same then it returns true if prepare position is greater than
// arguments prepare position.
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

// LessThan returns true if receiver's position is less than provided position.
// Position is less than if commit position is less than argument's commit position
// or if commit position is the same then it returns true if prepare position is
// less than argument's prepare position.
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
