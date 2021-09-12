package event_streams

import (
	system_metadata "github.com/EventStore/EventStore-Client-Go/systemmetadata"
	"github.com/gofrs/uuid"
)

type ProposedEvent struct {
	EventID      uuid.UUID
	EventType    string
	ContentType  string
	Data         []byte
	UserMetadata []byte
}

func (this ProposedEvent) ToProposedMessage() AppendRequestContentProposedMessage {
	metadata := map[string]string{}
	metadata[system_metadata.SystemMetadataKeysContentType] = this.ContentType
	metadata[system_metadata.SystemMetadataKeysType] = this.EventType
	return AppendRequestContentProposedMessage{
		Id:             this.EventID,
		Data:           this.Data,
		CustomMetadata: this.UserMetadata,
		Metadata:       metadata,
	}
}
