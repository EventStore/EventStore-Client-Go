package esgrpc

import uuid "github.com/gofrs/uuid"

// Message ...
type Message struct {
	EventID   uuid.UUID
	EventType string
	IsJSON    bool
	Data      []byte
	Metadata  map[string]string
}
