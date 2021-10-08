package persistent

import (
	"time"

	"github.com/google/uuid"
	"github.com/pivonroll/EventStore-Client-Go/position"
)

type ReadResponseEvent struct {
	Event          *RecordedEvent
	Link           *RecordedEvent
	CommitPosition *uint64 // nil if NoCommit Position is received
	RetryCount     *int32  // nil if NoRetryCount is received
}

func (responseEvent ReadResponseEvent) GetOriginalEvent() *RecordedEvent {
	if responseEvent.Link != nil {
		return responseEvent.Link
	}

	return responseEvent.Event
}

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
