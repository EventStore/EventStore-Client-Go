package persistent

import (
	"github.com/EventStore/EventStore-Client-Go/messages"
	"github.com/EventStore/EventStore-Client-Go/protos/persistent"
	system_metadata "github.com/EventStore/EventStore-Client-Go/systemmetadata"
)

type messageAdapter interface {
	FromProtoResponse(resp *persistent.ReadResp) *messages.RecordedEvent
}

type messageAdapterImpl struct{}

func (adapter messageAdapterImpl) FromProtoResponse(resp *persistent.ReadResp) *messages.RecordedEvent {
	event := resp.GetEvent()
	recordedEvent := event.GetEvent()

	message := newMessageFromProto(recordedEvent)
	return &message
}

func newMessageFromProto(recordedEvent *persistent.ReadResp_ReadEvent_RecordedEvent) messages.RecordedEvent {
	streamIdentifier := recordedEvent.GetStreamIdentifier()

	return messages.RecordedEvent{
		EventID:        eventIDFromProto(recordedEvent),
		EventType:      recordedEvent.Metadata[system_metadata.SystemMetadataKeysType],
		ContentType:    getContentTypeFromProto(recordedEvent),
		StreamID:       string(streamIdentifier.StreamName),
		EventNumber:    recordedEvent.GetStreamRevision(),
		CreatedDate:    createdFromProto(recordedEvent),
		Position:       positionFromProto(recordedEvent),
		Data:           recordedEvent.GetData(),
		SystemMetadata: recordedEvent.GetMetadata(),
		UserMetadata:   recordedEvent.GetCustomMetadata(),
	}
}
