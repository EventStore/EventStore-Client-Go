package persistent

import (
	"testing"

	"github.com/EventStore/EventStore-Client-Go/protos/persistent"
	"github.com/EventStore/EventStore-Client-Go/protos/shared"
	"github.com/stretchr/testify/require"
)

func Test_deleteRequestStreamProto(t *testing.T) {
	request := DeleteOptions{
		StreamName: []byte("stream name"),
		GroupName:  "group name",
	}
	result := deleteRequestStreamProto(request)

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
