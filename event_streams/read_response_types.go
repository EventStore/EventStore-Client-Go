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

// ReadResponse represents a response received when reading a stream.
// When reading a stream we can receive either an event or a checkpoint.
// Use #GetEvent and GetCheckpoint to determine the result stored in a response.
type ReadResponse struct {
	result isReadResponseResult
}

// GetEvent returns an event and a boolean which indicates if received value was actually an event.
// If returned boolean is false then returned event is a zero initialized ResolvedEvent.
func (response ReadResponse) GetEvent() (ResolvedEvent, bool) {
	event, isEvent := response.result.(ResolvedEvent)
	return event, isEvent
}

// GetCheckpoint returns a checkpoint and a boolean which indicates if received value was actually a
// checkpoint. If returned boolean is false then returned checkpoint is a zero
// initialized ReadResponseCheckpoint.
func (response ReadResponse) GetCheckpoint() (ReadResponseCheckpoint, bool) {
	checkpoint, isCheckpoint := response.result.(ReadResponseCheckpoint)
	return checkpoint, isCheckpoint
}

type isReadResponseResult interface {
	isReadResponseResult()
}

// StreamNotFoundError is an error returned if we tried to read a stream which does not exist.
type StreamNotFoundError struct {
	err      errors.Error
	streamId string
}

// Error returns a string representation of an error.
func (streamNotFound StreamNotFoundError) Error() string {
	return streamNotFound.err.Error()
}

// Code returns a code of an error. Use this to determine the error type.
func (streamNotFound StreamNotFoundError) Code() errors.ErrorCode {
	return streamNotFound.err.Code()
}

// GetStreamId returns a stream identifier of a stream which does not exist.
// This identifier was set in a read request set to EventStoreDB.
func (streamNotFound StreamNotFoundError) GetStreamId() string {
	return streamNotFound.streamId
}

// ReadResponseCheckpoint represents a checkpoint stored in a stream.
// Checkpoints are used to mark certain positions of interest in a stream.
type ReadResponseCheckpoint struct {
	CommitPosition  uint64
	PreparePosition uint64
}

func (this ReadResponseCheckpoint) isReadResponseResult() {}

// readResponseAdapter is used to construct read response from received protobuf message.
type readResponseAdapter interface {
	create(response *streams2.ReadResp) (ReadResponse, errors.Error)
}

type readResponseAdapterImpl struct{}

func (this readResponseAdapterImpl) create(protoResponse *streams2.ReadResp) (ReadResponse, errors.Error) {
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

		result.result = event
	case *streams2.ReadResp_Checkpoint_:
		protoCheckpointResponse := protoResponse.Content.(*streams2.ReadResp_Checkpoint_).Checkpoint
		result.result = ReadResponseCheckpoint{
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
