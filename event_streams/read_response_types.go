package event_streams

import (
	"fmt"
	"strconv"
	"time"

	"github.com/pivonroll/EventStore-Client-Go/errors"

	"github.com/pivonroll/EventStore-Client-Go/position"

	"github.com/pivonroll/EventStore-Client-Go/ptr"

	system_metadata "github.com/pivonroll/EventStore-Client-Go/systemmetadata"

	"github.com/google/uuid"
	"github.com/pivonroll/EventStore-Client-Go/protos/streams2"
)

type ReadResponse struct {
	// ResolvedEvent
	// ReadResponseCheckpoint
	Result isReadResponseResult
}

func (response ReadResponse) GetEvent() (ResolvedEvent, bool) {
	event, isEvent := response.Result.(ResolvedEvent)
	return event, isEvent
}

func (response ReadResponse) GetCheckpoint() (ReadResponseCheckpoint, bool) {
	checkpoint, isCheckpoint := response.Result.(ReadResponseCheckpoint)
	return checkpoint, isCheckpoint
}

type isReadResponseResult interface {
	isReadResponseResult()
}

type StreamNotFoundError struct {
	err      errors.Error
	streamId string
}

func (streamNotFound StreamNotFoundError) Error() string {
	return streamNotFound.err.Error()
}

func (streamNotFound StreamNotFoundError) Code() errors.ErrorCode {
	return streamNotFound.err.Code()
}

func (streamNotFound StreamNotFoundError) GetStreamId() string {
	return streamNotFound.streamId
}

type ReadResponseCheckpoint struct {
	CommitPosition  uint64
	PreparePosition uint64
}

func (this ReadResponseCheckpoint) isReadResponseResult() {}

type readResponseAdapter interface {
	Create(response *streams2.ReadResp) (ReadResponse, errors.Error)
}

type readResponseAdapterImpl struct{}

func (this readResponseAdapterImpl) Create(protoResponse *streams2.ReadResp) (ReadResponse, errors.Error) {
	result := ReadResponse{}

	switch protoResponse.Content.(type) {
	case *streams2.ReadResp_Event:
		protoEventResponse := protoResponse.Content.(*streams2.ReadResp_Event).Event
		event := ResolvedEvent{}

		event.Event = createRecordedEvent(protoEventResponse.Event)
		event.Link = createRecordedEvent(protoEventResponse.Link)

		switch protoEventResponse.Position.(type) {
		case *streams2.ReadResp_ReadEvent_CommitPosition:
			protoPosition := protoEventResponse.Position.(*streams2.ReadResp_ReadEvent_CommitPosition)
			event.CommitPosition = ptr.UInt64(protoPosition.CommitPosition)
		case *streams2.ReadResp_ReadEvent_NoPosition:
			event.CommitPosition = nil
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
		return ReadResponse{}, StreamNotFoundError{
			streamId: string(protoStreamNotFound.StreamIdentifier.StreamName),
			err:      errors.NewErrorCode(errors.StreamNotFoundErr),
		}
	default:
		if protoResponse.GetConfirmation() != nil {
			panic(fmt.Sprintf("Received stream confirmation for stream %s",
				protoResponse.GetConfirmation().SubscriptionId))
		}
		panic("Unexpected type received")
	}

	return result, nil
}

func createRecordedEvent(protoEvent *streams2.ReadResp_ReadEvent_RecordedEvent) *RecordedEvent {
	if protoEvent == nil {
		return nil
	}

	protoEventId := protoEvent.GetId().GetString_()

	contentType := protoEvent.Metadata[system_metadata.SystemMetadataKeysContentType]
	if contentType != string(ContentTypeJson) && contentType != string(ContentTypeOctetStream) {
		panic("Invalid content type ")
	}

	eventType := protoEvent.Metadata[system_metadata.SystemMetadataKeysType]

	systemMetadata := protoEvent.Metadata
	createdAtString := protoEvent.Metadata[system_metadata.SystemMetadataKeysCreated]
	createdAtDotNetTicks, err := strconv.ParseInt(createdAtString, 10, 64)
	if err != nil {
		panic(fmt.Sprintf("Invalid value received for date time. Value %s", createdAtString))
	}

	// The metadata contains the number of .NET "ticks" (100ns increments) since the UNIX epoch
	createdDateTime := time.Unix(0, createdAtDotNetTicks*100).UTC()

	delete(systemMetadata, system_metadata.SystemMetadataKeysType)
	delete(systemMetadata, system_metadata.SystemMetadataKeysContentType)

	return &RecordedEvent{
		EventID:     uuid.MustParse(protoEventId),
		EventType:   eventType,
		ContentType: ContentType(contentType),
		StreamId:    string(protoEvent.StreamIdentifier.StreamName),
		EventNumber: protoEvent.StreamRevision,
		Position: position.Position{
			Commit:  protoEvent.CommitPosition,
			Prepare: protoEvent.PreparePosition,
		},
		CreatedDateTime: createdDateTime,
		Data:            protoEvent.Data,
		SystemMetadata:  systemMetadata,
		UserMetadata:    protoEvent.CustomMetadata,
	}
}
