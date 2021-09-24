package persistent

import (
	"testing"

	"github.com/pivonroll/EventStore-Client-Go/protos/persistent"
	"github.com/pivonroll/EventStore-Client-Go/protos/shared"
	"github.com/stretchr/testify/require"
)

func Test_toPersistentReadRequest(t *testing.T) {
	expectedResult := &persistent.ReadReq{
		Content: &persistent.ReadReq_Options_{
			Options: &persistent.ReadReq_Options{
				BufferSize: 10,
				GroupName:  "some group",
				StreamOption: &persistent.ReadReq_Options_StreamIdentifier{
					StreamIdentifier: &shared.StreamIdentifier{
						StreamName: []byte("stream name"),
					},
				},
				UuidOption: &persistent.ReadReq_Options_UUIDOption{
					Content: &persistent.ReadReq_Options_UUIDOption_String_{
						String_: nil,
					},
				},
			},
		},
	}
	result := toPersistentReadRequest(10, "some group", "stream name")
	require.Equal(t, expectedResult, result)
}
