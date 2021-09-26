package persistent

import (
	"github.com/pivonroll/EventStore-Client-Go/protos/persistent"
	"github.com/pivonroll/EventStore-Client-Go/ptr"
	system_metadata "github.com/pivonroll/EventStore-Client-Go/systemmetadata"
)

type messageAdapter interface {
	fromProtoResponse(resp *persistent.ReadResp_ReadEvent) ReadResponseEvent
}

type messageAdapterImpl struct{}

func (adapter messageAdapterImpl) fromProtoResponse(readEvent *persistent.ReadResp_ReadEvent) ReadResponseEvent {
	positionWire := readEvent.GetPosition()
	eventWire := readEvent.GetEvent()
	linkWire := readEvent.GetLink()
	retryCount := readEvent.GetCount()

	result := ReadResponseEvent{
		Event:          nil,
		Link:           nil,
		CommitPosition: nil,
		RetryCount:     nil,
	}

	if eventWire != nil {
		event := newRecordedEventFromProto(eventWire)
		result.Event = &event
	}

	if linkWire != nil {
		link := newRecordedEventFromProto(linkWire)
		result.Link = &link
	}

	if retryCount != nil {
		result.RetryCount = ptr.Int32(readEvent.GetRetryCount())
	}

	if positionWire != nil {
		result.CommitPosition = ptr.UInt64(readEvent.GetCommitPosition())
	}

	return result
}

func newRecordedEventFromProto(
	recordedEvent *persistent.ReadResp_ReadEvent_RecordedEvent) RecordedEvent {
	streamIdentifier := recordedEvent.GetStreamIdentifier()

	return RecordedEvent{
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
