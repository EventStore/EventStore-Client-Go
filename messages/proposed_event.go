package messages

import (
	uuid "github.com/gofrs/uuid"
)

// ProposedEvent ...
type ProposedEvent struct {
	EventID      uuid.UUID
	EventType    string
	IsJSON       bool
	Data         []byte
	UserMetadata []byte
}
