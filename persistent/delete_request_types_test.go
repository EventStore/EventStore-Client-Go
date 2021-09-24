package persistent

import (
	"testing"

	"github.com/pivonroll/EventStore-Client-Go/protos/persistent"
	"github.com/pivonroll/EventStore-Client-Go/protos/shared"
	"github.com/stretchr/testify/require"
)

func Test_deleteRequestStreamProto(t *testing.T) {
	request := DeleteRequest{
		StreamName: "stream name",
		GroupName:  "group name",
	}
	result := request.Build()

	expectedResult := &persistent.DeleteReq{
		Options: &persistent.DeleteReq_Options{
			GroupName: "group name",
			StreamOption: &persistent.DeleteReq_Options_StreamIdentifier{
				StreamIdentifier: &shared.StreamIdentifier{
					StreamName: []byte("stream name"),
				},
			},
		},
	}
	require.Equal(t, expectedResult, result)
}

func Test_deleteRequestAllOptionsProto(t *testing.T) {
	groupName := "group name"
	result := deleteRequestAllOptionsProto(groupName)

	expectedResult := &persistent.DeleteReq{
		Options: &persistent.DeleteReq_Options{
			GroupName: groupName,
			StreamOption: &persistent.DeleteReq_Options_All{
				All: &shared.Empty{},
			},
		},
	}
	require.Equal(t, expectedResult, result)
}
