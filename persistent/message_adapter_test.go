package persistent

import (
	"strconv"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/pivonroll/EventStore-Client-Go/messages"
	"github.com/pivonroll/EventStore-Client-Go/position"
	"github.com/pivonroll/EventStore-Client-Go/protos/persistent"
	"github.com/pivonroll/EventStore-Client-Go/protos/shared"
	system_metadata "github.com/pivonroll/EventStore-Client-Go/systemmetadata"
	"github.com/stretchr/testify/require"
)

func Test_MessageAdapter(t *testing.T) {
	id, _ := uuid.NewV1()
	createdTime := time.Now().UTC()
	// truncate last two nanosecond digits since server delivers time in 100th nanosecond increments
	// use universal UTC time because different developers operate in different time zones
	createdTime = time.Unix(0, (createdTime.UnixNano()/100)*100).UTC()
	nanoseconds := createdTime.UnixNano()

	protoResponse := &persistent.ReadResp{
		Content: &persistent.ReadResp_Event{
			Event: &persistent.ReadResp_ReadEvent{
				Event: &persistent.ReadResp_ReadEvent_RecordedEvent{
					Id: &shared.UUID{
						Value: &shared.UUID_String_{
							String_: id.String(),
						},
					},
					StreamIdentifier: &shared.StreamIdentifier{
						StreamName: []byte("stream identifier"),
					},
					StreamRevision:  5,
					PreparePosition: 10,
					CommitPosition:  20,
					Metadata: map[string]string{
						system_metadata.SystemMetadataKeysType:        "some event type",
						system_metadata.SystemMetadataKeysContentType: "some content type",
						system_metadata.SystemMetadataKeysCreated:     strconv.FormatInt(nanoseconds/100, 10),
						"some_key": "some value",
					},
					CustomMetadata: []byte("some custom meta data"),
					Data:           []byte("some data"),
				},
			},
		},
	}

	expectedMessage := &messages.RecordedEvent{
		EventID:     id,
		EventType:   "some event type",
		ContentType: "some content type",
		StreamID:    "stream identifier",
		EventNumber: 5,
		Position: position.Position{
			Commit:  20,
			Prepare: 10,
		},
		CreatedDate: createdTime,
		Data:        []byte("some data"),
		SystemMetadata: map[string]string{
			system_metadata.SystemMetadataKeysType:        "some event type",
			system_metadata.SystemMetadataKeysContentType: "some content type",
			system_metadata.SystemMetadataKeysCreated:     strconv.FormatInt(nanoseconds/100, 10),
			"some_key": "some value",
		},
		UserMetadata: []byte("some custom meta data"),
	}

	messageAdapterInstance := messageAdapterImpl{}
	message := messageAdapterInstance.FromProtoResponse(protoResponse)

	// compare string representations of time
	require.Equal(t, expectedMessage.CreatedDate.String(), message.GetOriginalEvent().CreatedDate.String())
	// because it is hard to compare time in tests set this to be equal so that we can compare other fields
	expectedMessage.CreatedDate = message.GetOriginalEvent().CreatedDate

	require.Equal(t, expectedMessage, message.GetOriginalEvent())
}
