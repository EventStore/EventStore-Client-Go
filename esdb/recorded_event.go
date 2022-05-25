package esdb

import (
	"time"

	uuid "github.com/gofrs/uuid"
)

// RecordedEvent represents a previously written event.
type RecordedEvent struct {
	// Event's id.
	EventID uuid.UUID
	// Event's type.
	EventType string
	// Event's content type.
	ContentType string
	// The stream that event belongs to.
	StreamID string
	// The event's revision number.
	EventNumber uint64
	// The event's transaction log position.
	Position Position
	// When the event was created.
	CreatedDate time.Time
	// The event's payload data.
	Data []byte
	// The event's system metadata.
	SystemMetadata map[string]string
	// The event user-defined metadata.
	UserMetadata []byte
}
