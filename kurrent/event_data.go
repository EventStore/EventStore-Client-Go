package kurrent

import "github.com/google/uuid"

// ContentType event's content type.
type ContentType int

const (
	// ContentTypeBinary binary content type.
	ContentTypeBinary ContentType = 0
	// ContentTypeJson JSON content type.
	ContentTypeJson ContentType = 1
)

// EventData represents an event that will be sent to KurrentDB.
type EventData struct {
	// Event's unique identifier.
	EventID uuid.UUID
	// Event's type.
	EventType string
	// Event's content type.
	ContentType ContentType
	// Event's payload data.
	Data []byte
	// Event's metadata.
	Metadata []byte
}
