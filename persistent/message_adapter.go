package persistent

import (
	"github.com/pivonroll/EventStore-Client-Go/messages"
	"github.com/pivonroll/EventStore-Client-Go/protos/persistent"
	system_metadata "github.com/pivonroll/EventStore-Client-Go/systemmetadata"
)

type messageAdapter interface {
	FromProtoResponse(resp *persistent.ReadResp) *messages.ResolvedEvent
}

type messageAdapterImpl struct{}

func (adapter messageAdapterImpl) FromProtoResponse(resp *persistent.ReadResp) *messages.ResolvedEvent {
	readEvent := resp.GetEvent()
	positionWire := readEvent.GetPosition()
	eventWire := readEvent.GetEvent()
	linkWire := readEvent.GetLink()

	var event *messages.RecordedEvent = nil
	var link *messages.RecordedEvent = nil
	var commit *uint64

	if positionWire != nil {
		switch value := positionWire.(type) {
		case *persistent.ReadResp_ReadEvent_CommitPosition:
			{
				commit = &value.CommitPosition
			}
		case *persistent.ReadResp_ReadEvent_NoPosition:
			{
				commit = nil
			}
		}
	}

	if eventWire != nil {
		recordedEvent := newMessageFromProto(eventWire)
		event = &recordedEvent
	}

	if linkWire != nil {
		recordedEvent := newMessageFromProto(linkWire)
		link = &recordedEvent
	}

	return &messages.ResolvedEvent{
		Event:  event,
		Link:   link,
		Commit: commit,
	}
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
