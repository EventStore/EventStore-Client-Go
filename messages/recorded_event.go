package messages

import (
	"time"

	uuid "github.com/gofrs/uuid"
	position "github.com/pivonroll/EventStore-Client-Go/position"
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
