package event_streams

import (
	system_metadata "github.com/EventStore/EventStore-Client-Go/systemmetadata"
	"github.com/gofrs/uuid"
)

type ProposedEvent struct {
	EventID      uuid.UUID
	EventType    string
	ContentType  ContentType
	Data         []byte
	UserMetadata []byte
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
