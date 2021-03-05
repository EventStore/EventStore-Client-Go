package messages

// CheckpointEvent ...
type CheckpointEvent struct {
	Commit  uint64
	Prepare uint64
}
