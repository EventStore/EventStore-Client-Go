package protoutils

import (
	"log"
	"strconv"
	"time"

	direction "github.com/eventstore/EventStore-Client-Go/direction"
	messages "github.com/eventstore/EventStore-Client-Go/messages"
	position "github.com/eventstore/EventStore-Client-Go/position"
	api "github.com/eventstore/EventStore-Client-Go/protos"
	stream_revision "github.com/eventstore/EventStore-Client-Go/streamrevision"
	system_metadata "github.com/eventstore/EventStore-Client-Go/systemmetadata"
	"github.com/gofrs/uuid"
)

//ToAppendHeaderFromStreamIDAndStreamRevision ...
func ToAppendHeaderFromStreamIDAndStreamRevision(streamID string, streamRevision stream_revision.StreamRevision) *api.AppendReq {
	appendReq := &api.AppendReq{
		Content: &api.AppendReq_Options_{
			Options: &api.AppendReq_Options{},
		},
	}
	appendReq.GetOptions().StreamName = streamID
	switch streamRevision {
	case stream_revision.StreamRevisionAny:
		appendReq.GetOptions().ExpectedStreamRevision = &api.AppendReq_Options_Any{
			Any: &api.AppendReq_Empty{},
		}
	case stream_revision.StreamRevisionNoStream:
		appendReq.GetOptions().ExpectedStreamRevision = &api.AppendReq_Options_NoStream{
			NoStream: &api.AppendReq_Empty{},
		}
	case stream_revision.StreamRevisionStreamExists:
		appendReq.GetOptions().ExpectedStreamRevision = &api.AppendReq_Options_StreamExists{
			StreamExists: &api.AppendReq_Empty{},
		}
	default:
		appendReq.GetOptions().ExpectedStreamRevision = &api.AppendReq_Options_Revision{
			Revision: uint64(streamRevision),
		}
	}
	return appendReq
}

//ToProposedMessage ...
func ToProposedMessage(event messages.ProposedEvent) *api.AppendReq_ProposedMessage {
	metadata := map[string]string{}
	if event.IsJSON {
		metadata[system_metadata.SystemMetadataKeysIsJSON] = "true"
	} else {
		metadata[system_metadata.SystemMetadataKeysIsJSON] = "false"
	}
	metadata[system_metadata.SystemMetadataKeysType] = event.EventType
	return &api.AppendReq_ProposedMessage{
		Id: &api.UUID{
			Value: &api.UUID_String_{
				String_: event.EventID.String(),
			},
		},
		Data:           event.Data,
		CustomMetadata: event.UserMetadata,
		Metadata:       metadata,
	}
}

//ToReadDirectionFromDirection ...
func ToReadDirectionFromDirection(dir direction.Direction) api.ReadReq_Options_ReadDirection {
	var readDirection api.ReadReq_Options_ReadDirection
	switch dir {
	case direction.Forwards:
		readDirection = api.ReadReq_Options_Forwards
	case direction.Backwards:
		readDirection = api.ReadReq_Options_Backwards
	}
	return readDirection
}

//ToAllReadOptionsFromPosition ...
func ToAllReadOptionsFromPosition(position position.Position) *api.ReadReq_Options_All {
	return &api.ReadReq_Options_All{
		All: &api.ReadReq_Options_AllOptions{
			AllOption: &api.ReadReq_Options_AllOptions_Position{
				Position: &api.ReadReq_Options_Position{
					PreparePosition: uint64(position.Prepare),
					CommitPosition:  uint64(position.Commit),
				},
			},
		},
	}
}

// ToReadStreamOptionsFromStreamAndStreamRevision ...
func ToReadStreamOptionsFromStreamAndStreamRevision(streamID string, streamRevision uint64) *api.ReadReq_Options_Stream {
	return &api.ReadReq_Options_Stream{
		Stream: &api.ReadReq_Options_StreamOptions{
			StreamName: streamID,
			RevisionOption: &api.ReadReq_Options_StreamOptions_Revision{
				Revision: uint64(streamRevision),
			},
		},
	}
}

//EventIDFromProto ...
func EventIDFromProto(recordedEvent *api.ReadResp_ReadEvent_RecordedEvent) uuid.UUID {
	id := recordedEvent.GetId()
	idString := id.GetString_()
	return uuid.FromStringOrNil(idString)
}

//CreatedFromProto ...
func CreatedFromProto(recordedEvent *api.ReadResp_ReadEvent_RecordedEvent) time.Time {
	timeSinceEpoch, err := strconv.ParseInt(recordedEvent.Metadata[system_metadata.SystemMetadataKeysCreated], 10, 64)
	if err != nil {
		log.Fatalf("Failed to parse created date as int from %+v", recordedEvent.Metadata[system_metadata.SystemMetadataKeysCreated])
	}
	// The metadata contains the number of .NET "ticks" (100ns increments) since the UNIX epoch
	return time.Unix(0, timeSinceEpoch*100).UTC()
}

//PositionFromProto ...
func PositionFromProto(recordedEvent *api.ReadResp_ReadEvent_RecordedEvent) position.Position {
	return position.Position{Commit: int64(recordedEvent.GetCommitPosition()), Prepare: int64(recordedEvent.GetPreparePosition())}
}

//IsJSONFromProto ...
func IsJSONFromProto(recordedEvent *api.ReadResp_ReadEvent_RecordedEvent) bool {
	isJSON, err := strconv.ParseBool(recordedEvent.Metadata[system_metadata.SystemMetadataKeysIsJSON])
	if err != nil {
		log.Fatalf("Failed to parse isJSON as bool from %+v", recordedEvent.Metadata[system_metadata.SystemMetadataKeysIsJSON])
	}
	return isJSON
}
