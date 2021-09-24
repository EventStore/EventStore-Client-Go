package event_streams

import (
	"fmt"
	"reflect"

	system_metadata "github.com/pivonroll/EventStore-Client-Go/systemmetadata"

	"github.com/gofrs/uuid"
	"github.com/pivonroll/EventStore-Client-Go/protos/streams2"
)

type ReadResponse struct {
	// ReadResponseEvent
	// ReadResponseCheckpoint
	// ReadResponseStreamNotFound
	Result isReadResponseResult
}

func (response ReadResponse) GetEvent() (ReadResponseEvent, bool) {
	event, isEvent := response.Result.(ReadResponseEvent)
	return event, isEvent
}

func (response ReadResponse) GetStreamNotFound() (ReadResponseStreamNotFound, bool) {
	streamNotFound, isEvent := response.Result.(ReadResponseStreamNotFound)
	return streamNotFound, isEvent
}

func (response ReadResponse) GetCheckpoint() (ReadResponseCheckpoint, bool) {
	checkpoint, isCheckpoint := response.Result.(ReadResponseCheckpoint)
	return checkpoint, isCheckpoint
}

type isReadResponseResult interface {
	isReadResponseResult()
}

type ReadResponseStreamNotFound struct {
	StreamId string
}

func (this ReadResponseStreamNotFound) isReadResponseResult() {}

type ReadResponseEventList []ReadResponseEvent

func (list ReadResponseEventList) Reverse() ReadResponseEventList {
	result := make(ReadResponseEventList, len(list))
	copy(result, list)
	n := reflect.ValueOf(result).Len()
	swap := reflect.Swapper(result)
	for i, j := 0, n-1; i < j; i, j = i+1, j-1 {
		swap(i, j)
	}

	return result
}

func (list ReadResponseEventList) ToProposedEvents() []ProposedEvent {
	var result []ProposedEvent

	for _, responseEvent := range list {
		result = append(result, responseEvent.ToProposedEvent())
	}

	return result
}

type ReadResponseEvent struct {
	Event *ReadResponseRecordedEvent
	Link  *ReadResponseRecordedEvent
	// Types that are assignable to Position:
	//	ReadResponseEventCommitPosition
	//	ReadResponseEventNoPosition
	Position IsReadResponsePosition
}

func (this ReadResponseEvent) ToProposedEvent() ProposedEvent {
	// metadata[system_metadata.SystemMetadataKeysContentType] = string(this.ContentType)
	// metadata[system_metadata.SystemMetadataKeysType] = this.EventType
	contentType := this.Event.Metadata[system_metadata.SystemMetadataKeysContentType]
	if contentType != string(ContentTypeJson) && contentType != string(ContentTypeOctetStream) {
		panic("Invalid content type ")
	}
	return ProposedEvent{
		EventID:      this.Event.Id,
		EventType:    this.Event.Metadata[system_metadata.SystemMetadataKeysType],
		ContentType:  ContentType(contentType),
		Data:         this.Event.Data,
		UserMetadata: this.Event.CustomMetadata,
	}
}

func (this ReadResponseEvent) isReadResponseResult() {}

func (this ReadResponseEvent) GetCommitPosition() (uint64, bool) {
	if commitPosition, isCommitPosition := this.Position.(ReadResponseEventCommitPosition); isCommitPosition {
		return commitPosition.CommitPosition, true
	}

	return 0, false
}

type IsReadResponsePosition interface {
	isReadResponsePosition()
}

type ReadResponseEventCommitPosition struct {
	CommitPosition uint64
}

func (this ReadResponseEventCommitPosition) isReadResponsePosition() {}

type ReadResponseEventNoPosition struct{}

func (this ReadResponseEventNoPosition) isReadResponsePosition() {}

type ReadResponseRecordedEvent struct {
	Id               uuid.UUID
	StreamIdentifier string
	StreamRevision   uint64
	PreparePosition  uint64
	CommitPosition   uint64
	Metadata         map[string]string
	CustomMetadata   []byte
	Data             []byte
}

type ReadResponseCheckpoint struct {
	CommitPosition  uint64
	PreparePosition uint64
}

func (this ReadResponseCheckpoint) isReadResponseResult() {}

type readResponseAdapter interface {
	Create(response *streams2.ReadResp) ReadResponse
}

type readResponseAdapterImpl struct{}

func (this readResponseAdapterImpl) Create(protoResponse *streams2.ReadResp) ReadResponse {
	result := ReadResponse{}

	switch protoResponse.Content.(type) {
	case *streams2.ReadResp_Event:
		protoEventResponse := protoResponse.Content.(*streams2.ReadResp_Event).Event
		event := ReadResponseEvent{}

		if protoEventResponse.Event != nil {
			protoEvent := protoEventResponse.Event
			id := protoEvent.GetId()
			idString := id.GetString_()

			event.Event = &ReadResponseRecordedEvent{
				Id:               uuid.FromStringOrNil(idString),
				StreamIdentifier: string(protoEvent.StreamIdentifier.StreamName),
				StreamRevision:   protoEvent.StreamRevision,
				PreparePosition:  protoEvent.PreparePosition,
				CommitPosition:   protoEvent.CommitPosition,
				Metadata:         protoEvent.Metadata,
				CustomMetadata:   protoEvent.CustomMetadata,
				Data:             protoEvent.Data,
			}
		}

		if protoEventResponse.Link != nil {
			protoEventLink := protoEventResponse.Link
			id := protoEventLink.GetId()
			idString := id.GetString_()

			event.Link = &ReadResponseRecordedEvent{
				Id:               uuid.FromStringOrNil(idString),
				StreamIdentifier: string(protoEventLink.StreamIdentifier.StreamName),
				StreamRevision:   protoEventLink.StreamRevision,
				PreparePosition:  protoEventLink.PreparePosition,
				CommitPosition:   protoEventLink.CommitPosition,
				Metadata:         protoEventLink.Metadata,
				CustomMetadata:   protoEventLink.CustomMetadata,
				Data:             protoEventLink.Data,
			}
		}

		switch protoEventResponse.Position.(type) {
		case *streams2.ReadResp_ReadEvent_CommitPosition:
			protoPosition := protoEventResponse.Position.(*streams2.ReadResp_ReadEvent_CommitPosition)
			event.Position = ReadResponseEventCommitPosition{
				CommitPosition: protoPosition.CommitPosition,
			}
		case *streams2.ReadResp_ReadEvent_NoPosition:
			event.Position = ReadResponseEventNoPosition{}
		}

		result.Result = event
	case *streams2.ReadResp_Checkpoint_:
		protoCheckpointResponse := protoResponse.Content.(*streams2.ReadResp_Checkpoint_).Checkpoint
		result.Result = ReadResponseCheckpoint{
			CommitPosition:  protoCheckpointResponse.CommitPosition,
			PreparePosition: protoCheckpointResponse.PreparePosition,
		}
	case *streams2.ReadResp_StreamNotFound_:
		protoStreamNotFound := protoResponse.Content.(*streams2.ReadResp_StreamNotFound_).StreamNotFound
		result.Result = ReadResponseStreamNotFound{
			StreamId: string(protoStreamNotFound.StreamIdentifier.StreamName),
		}

	default:
		if protoResponse.GetConfirmation() != nil {
			panic(fmt.Sprintf("Received stream confirmation for stream %s",
				protoResponse.GetConfirmation().SubscriptionId))
		}
		panic("Unexpected type received")
	}

	return result
}
