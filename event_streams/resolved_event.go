package event_streams

import (
	"reflect"
	"time"

	"github.com/gofrs/uuid"
	"github.com/pivonroll/EventStore-Client-Go/position"
)

type ResolvedEvent struct {
	Link           *RecordedEvent
	Event          *RecordedEvent
	CommitPosition *uint64 // nil if no position
}

func (this ResolvedEvent) isReadResponseResult() {}

func (resolved ResolvedEvent) GetOriginalEvent() *RecordedEvent {
	if resolved.Link != nil {
		return resolved.Link
	}

	return resolved.Event
}

func (this ResolvedEvent) ToProposedEvent() ProposedEvent {
	event := this.GetOriginalEvent()

	return ProposedEvent{
		EventID:      event.EventID,
		EventType:    event.EventType,
		ContentType:  event.ContentType,
		Data:         event.Data,
		UserMetadata: event.UserMetadata,
	}
}

type ResolvedEventList []ResolvedEvent

func (list ResolvedEventList) Reverse() ResolvedEventList {
	result := make(ResolvedEventList, len(list))
	copy(result, list)
	n := reflect.ValueOf(result).Len()
	swap := reflect.Swapper(result)
	for i, j := 0, n-1; i < j; i, j = i+1, j-1 {
		swap(i, j)
	}

	return result
}

func (list ResolvedEventList) ToProposedEvents() ProposedEventList {
	var result ProposedEventList

	for _, item := range list {
		result = append(result, item.ToProposedEvent())
	}

	return result
}

type RecordedEvent struct {
	EventID         uuid.UUID
	EventType       string
	ContentType     ContentType
	StreamId        string
	EventNumber     uint64
	Position        position.Position
	CreatedDateTime time.Time
	Data            []byte
	SystemMetadata  map[string]string
	UserMetadata    []byte
}
