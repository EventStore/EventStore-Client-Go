package esdb

import (
	uuid "github.com/gofrs/uuid"
)

type ContentType int

const (
	BinaryContentType ContentType = 0
	JsonContentType   ContentType = 1
)

// EventData ...
type EventData struct {
	EventID     uuid.UUID
	EventType   string
	ContentType ContentType
	Data        []byte
	Metadata    []byte
}
