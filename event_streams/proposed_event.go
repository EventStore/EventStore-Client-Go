package event_streams

import (
	"github.com/google/uuid"
	system_metadata "github.com/pivonroll/EventStore-Client-Go/systemmetadata"
)

// ProposedEvent represents an event we want to append to a stream.
// EventId is a unique id of an event.
// EventType field is a user defined event type.
// ContentType will tell EventStoreDB how to store this event. Event can be stored as json or as octet-stream.
// Data are user defined data to be stored in an event.
// UserMetadata holds user defined metadata for an event.
type ProposedEvent struct {
	EventId      uuid.UUID
	EventType    string
	ContentType  ContentType
	Data         []byte
	UserMetadata []byte
}

// ProposedEventList represents a slice of events.
type ProposedEventList []ProposedEvent

func (list ProposedEventList) toBatchAppendRequestChunks(chunkSize uint64) [][]batchAppendRequestProposedMessage {
	if chunkSize == 0 {
		panic("Chunk size cannot be zero")
	}

	var result [][]batchAppendRequestProposedMessage
	temp := list
	for {
		if len(temp) == 0 {
			break
		}
		rightIndex := chunkSize
		if uint64(len(temp)) < rightIndex {
			rightIndex = uint64(len(temp))
		}
		batch := temp[:rightIndex]
		result = append(result, batch.toBatchAppendRequestList())
		if uint64(len(batch)) < chunkSize {
			break
		}
		temp = temp[rightIndex:]
	}

	if len(result) == 0 {
		result = append(result, []batchAppendRequestProposedMessage{})
	}

	return result
}

func (list ProposedEventList) toBatchAppendRequestList() []batchAppendRequestProposedMessage {
	var result []batchAppendRequestProposedMessage
	for _, item := range list {
		result = append(result, item.toBatchMessage())
	}

	return result
}

// ContentType represents the content type of the events stored in EventStoreDB.
// EventStoreDB can store events in a json format or in octet-stream format.
type ContentType string

const (
	ContentTypeJson        ContentType = "application/json"
	ContentTypeOctetStream ContentType = "application/octet-stream"
)

func (this ProposedEvent) toProposedMessage() appendRequestContentProposedMessage {
	metadata := map[string]string{}
	metadata[system_metadata.SystemMetadataKeysContentType] = string(this.ContentType)
	metadata[system_metadata.SystemMetadataKeysType] = this.EventType
	return appendRequestContentProposedMessage{
		eventId:        this.EventId,
		data:           this.Data,
		customMetadata: this.UserMetadata,
		metadata:       metadata,
	}
}

func (this ProposedEvent) toBatchMessage() batchAppendRequestProposedMessage {
	metadata := map[string]string{}
	metadata[system_metadata.SystemMetadataKeysContentType] = string(this.ContentType)
	metadata[system_metadata.SystemMetadataKeysType] = this.EventType
	return batchAppendRequestProposedMessage{
		Id:             this.EventId,
		Data:           this.Data,
		CustomMetadata: this.UserMetadata,
		Metadata:       metadata,
	}
}
