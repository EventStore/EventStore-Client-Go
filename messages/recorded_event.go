package messages

import (
	position "github.com/eventstore/EventStore-Client-Go/position"
	uuid "github.com/gofrs/uuid"
	"time"
)

// RecordedEvent ...
type RecordedEvent struct {
	EventID        uuid.UUID
	EventType      string
	IsJSON         bool
	StreamID       string
	StreamRevision uint64
	Position       position.Position
	CreatedDate    time.Time
	Data           []byte
	SystemMetadata map[string]string
	UserMetadata   []byte
}
