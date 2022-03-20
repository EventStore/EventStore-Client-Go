package esdb

type ResolvedEvent struct {
	Link   *RecordedEvent
	Event  *RecordedEvent
	Commit *uint64
}

func (resolved ResolvedEvent) OriginalEvent() *RecordedEvent {
	if resolved.Link != nil {
		return resolved.Link
	}

	return resolved.Event
}
