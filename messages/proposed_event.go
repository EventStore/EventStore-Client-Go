package messages

import (
	uuid "github.com/gofrs/uuid"
)

// ProposedEvent ...
type ProposedEvent struct {
	EventID      uuid.UUID
	EventType    string
	ContentType  string
	Data         []byte
	UserMetadata []byte
}
