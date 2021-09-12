package event_streams

import (
	"github.com/EventStore/EventStore-Client-Go/protos/streams2"
	"github.com/gofrs/uuid"
)

type ReadResponse struct {
	// ReadResponseEvent
	// ReadResponseConfirmation
	// ReadResponseCheckpoint
	// ReadResponseStreamNotFound
	Result isReadResponseResult
}

type isReadResponseResult interface {
	isReadResponseResult()
}

type ReadResponseEvent struct {
	Event *ReadResponseRecordedEvent
	Link  *ReadResponseRecordedEvent
	// Types that are assignable to Position:
	//	ReadResponseEventCommitPosition
	//	ReadResponseEventNoPosition
	Position isReadResponsePosition
}

func (this ReadResponseEvent) isReadResponseResult() {}

type isReadResponsePosition interface {
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
	StreamIdentifier []byte
	StreamRevision   uint64
	PreparePosition  uint64
	CommitPosition   uint64
	Metadata         map[string]string
	CustomMetadata   []byte
	Data             []byte
}

type ReadResponseConfirmation struct {
	SubscriptionId string
}

func (this ReadResponseConfirmation) isReadResponseResult() {}

type ReadResponseCheckpoint struct {
	CommitPosition  uint64
	PreparePosition uint64
}

func (this ReadResponseCheckpoint) isReadResponseResult() {}

type ReadResponseStreamNotFound struct {
	StreamIdentifier []byte
}

func (this ReadResponseStreamNotFound) isReadResponseResult() {}

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
				StreamIdentifier: protoEvent.StreamIdentifier.StreamName,
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
				StreamIdentifier: protoEventLink.StreamIdentifier.StreamName,
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
	case *streams2.ReadResp_Confirmation:
		protoConfirmationResponse := protoResponse.Content.(*streams2.ReadResp_Confirmation).Confirmation
		result.Result = ReadResponseConfirmation{
			SubscriptionId: protoConfirmationResponse.SubscriptionId,
		}

	case *streams2.ReadResp_Checkpoint_:
		protoCheckpointResponse := protoResponse.Content.(*streams2.ReadResp_Checkpoint_).Checkpoint
		result.Result = ReadResponseCheckpoint{
			CommitPosition:  protoCheckpointResponse.CommitPosition,
			PreparePosition: protoCheckpointResponse.PreparePosition,
		}

	case *streams2.ReadResp_StreamNotFound_:
		protoNotFound := protoResponse.Content.(*streams2.ReadResp_StreamNotFound_).StreamNotFound
		result.Result = ReadResponseStreamNotFound{
			StreamIdentifier: protoNotFound.StreamIdentifier.StreamName,
		}
	}

	return result
}
