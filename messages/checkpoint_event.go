package messages

// CheckpointEvent ...
type CheckpointEvent struct {
	CommitPosition  uint64
	PreparePosition uint64
}
