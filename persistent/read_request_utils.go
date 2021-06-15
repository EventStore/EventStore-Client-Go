package persistent

import (
	"github.com/EventStore/EventStore-Client-Go/protos/persistent"
	"github.com/EventStore/EventStore-Client-Go/protos/shared"
)

func ToPersistentReadRequest(
	bufferSize int32,
	groupName string,
	streamName []byte,
) *persistent.ReadReq {
	return &persistent.ReadReq{
		Content: &persistent.ReadReq_Options_{
			Options: &persistent.ReadReq_Options{
				BufferSize: bufferSize,
				GroupName:  groupName,
				StreamOption: &persistent.ReadReq_Options_StreamIdentifier{
					StreamIdentifier: &shared.StreamIdentifier{
						StreamName: streamName,
					},
				},
			},
		},
	}
}
