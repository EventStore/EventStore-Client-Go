package messages

import (
	"time"

	position "github.com/EventStore/EventStore-Client-Go/position"
	uuid "github.com/gofrs/uuid"
)

// RecordedEvent ...
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
