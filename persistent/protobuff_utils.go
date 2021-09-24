package persistent

import (
	"log"
	"strconv"
	"time"

	"github.com/gofrs/uuid"
	"github.com/pivonroll/EventStore-Client-Go/position"
	"github.com/pivonroll/EventStore-Client-Go/protos/persistent"
	"github.com/pivonroll/EventStore-Client-Go/protos/shared"
	system_metadata "github.com/pivonroll/EventStore-Client-Go/systemmetadata"
)

func eventIDFromProto(recordedEvent *persistent.ReadResp_ReadEvent_RecordedEvent) uuid.UUID {
	id := recordedEvent.GetId()
	idString := id.GetString_()
	return uuid.FromStringOrNil(idString)
}

func toProtoUUID(id uuid.UUID) *shared.UUID {
	return &shared.UUID{
		Value: &shared.UUID_String_{
			String_: id.String(),
		},
	}
}

func getContentTypeFromProto(recordedEvent *persistent.ReadResp_ReadEvent_RecordedEvent) string {
	return recordedEvent.Metadata[system_metadata.SystemMetadataKeysContentType]
}

func createdFromProto(recordedEvent *persistent.ReadResp_ReadEvent_RecordedEvent) time.Time {
	timeSinceEpoch, err := strconv.ParseInt(
		recordedEvent.Metadata[system_metadata.SystemMetadataKeysCreated], 10, 64)
	if err != nil {
		log.Fatalf("Failed to parse created date as int from %+v",
			recordedEvent.Metadata[system_metadata.SystemMetadataKeysCreated])
	}
	// The metadata contains the number of .NET "ticks" (100ns increments) since the UNIX epoch
	return time.Unix(0, timeSinceEpoch*100).UTC()
}

func positionFromProto(recordedEvent *persistent.ReadResp_ReadEvent_RecordedEvent) position.Position {
	return position.Position{
		Commit:  recordedEvent.GetCommitPosition(),
		Prepare: recordedEvent.GetPreparePosition(),
	}
}
