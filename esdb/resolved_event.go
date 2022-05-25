package esdb

// ResolvedEvent represents an event with a potential link.
type ResolvedEvent struct {
	// The link event if the original event is a link.
	Link *RecordedEvent
	// The event, or the resolved linked event if the original event is a link.
	Event *RecordedEvent
	// The optional commit position of the event.
	Commit *uint64
}

// OriginalEvent returns the event that was read or which triggered the subscription.
func (resolved ResolvedEvent) OriginalEvent() *RecordedEvent {
	if resolved.Link != nil {
		return resolved.Link
	}

	return resolved.Event
}
