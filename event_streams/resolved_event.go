package event_streams

import (
	"time"

	"github.com/gofrs/uuid"
	"github.com/pivonroll/EventStore-Client-Go/position"
)

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

type RecordedEvent struct {
	EventID        uuid.UUID
	EventType      string
	ContentType    string
	StreamID       string
	EventNumber    uint64
	Position       position.Position
	CreatedDate    time.Time
	Data           []byte
	SystemMetadata map[string]string
	UserMetadata   []byte
}
