package event_streams

// WriteResult ...
type WriteResult struct {
	CommitPosition      uint64
	PreparePosition     uint64
	NextExpectedVersion uint64
}
