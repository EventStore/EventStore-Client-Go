package event_streams

import (
	"reflect"
	"time"

	"github.com/google/uuid"
	"github.com/pivonroll/EventStore-Client-Go/position"
)

// ResolvedEvent is an event received from a stream.
// Each event has either Event or Link set.
// If event has no commit position CommitPosition will be nil.
type ResolvedEvent struct {
	Link           *RecordedEvent
	Event          *RecordedEvent
	CommitPosition *uint64 // nil if no position
}

func (this ResolvedEvent) isReadResponseResult() {}

// GetOriginalEvent returns an original event.
// It chooses between Link and Event fields.
// Link field has precedence over Event field.
func (resolved ResolvedEvent) GetOriginalEvent() *RecordedEvent {
	if resolved.Link != nil {
		return resolved.Link
	}

	return resolved.Event
}

// ToProposedEvent returns event converted to ProposedEvent.
func (this ResolvedEvent) ToProposedEvent() ProposedEvent {
	event := this.GetOriginalEvent()

	return ProposedEvent{
		EventId:      event.EventID,
		EventType:    event.EventType,
		ContentType:  event.ContentType,
		Data:         event.Data,
		UserMetadata: event.UserMetadata,
	}
}

// ResolvedEventList is a shorthand type for slice of events.
type ResolvedEventList []ResolvedEvent

// Reverse returns a reversed slice of events.
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

// ToProposedEvents returns a slice of events, where each event is converted to ProposedEvent.
func (list ResolvedEventList) ToProposedEvents() ProposedEventList {
	var result ProposedEventList

	for _, item := range list {
		result = append(result, item.ToProposedEvent())
	}

	return result
}

// RecordedEvent represents an event recorded in the EventStoreDB.
type RecordedEvent struct {
	EventID         uuid.UUID         // ID of an event. Event's ID is provided by user when event is appended to a stream
	EventType       string            // user defined event type
	ContentType     ContentType       // content type used to store event in EventStoreDB. Supported types are ContentTypeJson and ContentTypeOctetStream
	StreamId        string            // stream identifier of a stream on which this event is stored
	EventNumber     uint64            // index number of an event in a stream
	Position        position.Position // event's position in stream $all
	CreatedDateTime time.Time         // a date and time when event was stored in a stream
	Data            []byte            // user data stored in an event.
	SystemMetadata  map[string]string // EventStoreDB's metadata set for an event.
	UserMetadata    []byte            // user defined metadata.
}
