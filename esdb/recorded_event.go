package esdb

import (
	"time"

	uuid "github.com/gofrs/uuid"
)

// RecordedEvent ...
type RecordedEvent struct {
	EventID        uuid.UUID
	EventType      string
	ContentType    string
	StreamID       string
	EventNumber    uint64
	Position       Position
	CreatedDate    time.Time
	Data           []byte
	SystemMetadata map[string]string
	UserMetadata   []byte
}
