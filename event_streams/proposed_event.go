package event_streams

import (
	"github.com/gofrs/uuid"
	system_metadata "github.com/pivonroll/EventStore-Client-Go/systemmetadata"
)

type ProposedEvent struct {
	EventID      uuid.UUID
	EventType    string
	ContentType  ContentType
	Data         []byte
	UserMetadata []byte
}

type ProposedEventList []ProposedEvent

func (list ProposedEventList) toBatchAppendRequestChunks(chunkSize uint64) [][]BatchAppendRequestProposedMessage {
	if chunkSize == 0 {
		panic("Chunk size cannot be zero")
	}

	var result [][]BatchAppendRequestProposedMessage
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
		result = append(result, []BatchAppendRequestProposedMessage{})
	}

	return result
}

func (list ProposedEventList) toBatchAppendRequestList() []BatchAppendRequestProposedMessage {
	var result []BatchAppendRequestProposedMessage
	for _, item := range list {
		result = append(result, item.ToBatchMessage())
	}

	return result
}

type ContentType string

const (
	ContentTypeJson        ContentType = "application/json"
	ContentTypeOctetStream ContentType = "application/octet-stream"
)

func (this ProposedEvent) ToProposedMessage() AppendRequestContentProposedMessage {
	metadata := map[string]string{}
	metadata[system_metadata.SystemMetadataKeysContentType] = string(this.ContentType)
	metadata[system_metadata.SystemMetadataKeysType] = this.EventType
	return AppendRequestContentProposedMessage{
		Id:             this.EventID,
		Data:           this.Data,
		CustomMetadata: this.UserMetadata,
		Metadata:       metadata,
	}
}

func (this ProposedEvent) ToBatchMessage() BatchAppendRequestProposedMessage {
	metadata := map[string]string{}
	metadata[system_metadata.SystemMetadataKeysContentType] = string(this.ContentType)
	metadata[system_metadata.SystemMetadataKeysType] = this.EventType
	return BatchAppendRequestProposedMessage{
		Id:             this.EventID,
		Data:           this.Data,
		CustomMetadata: this.UserMetadata,
		Metadata:       metadata,
	}
}
