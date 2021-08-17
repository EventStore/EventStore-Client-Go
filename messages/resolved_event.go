package messages

type ResolvedEvent struct {
	Link   *RecordedEvent
	Event  *RecordedEvent
	Commit *uint64
}

func (resolved ResolvedEvent) GetOriginalEvent() *RecordedEvent {
	if resolved.Link != nil {
		return resolved.Link
	}

	return resolved.Event
}
