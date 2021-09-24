package persistent

import (
	"github.com/pivonroll/EventStore-Client-Go/protos/persistent"
	"github.com/pivonroll/EventStore-Client-Go/protos/shared"
)

func toPersistentReadRequest(
	bufferSize int32,
	groupName string,
	streamName string,
) *persistent.ReadReq {
	return &persistent.ReadReq{
		Content: &persistent.ReadReq_Options_{
			Options: &persistent.ReadReq_Options{
				BufferSize: bufferSize,
				GroupName:  groupName,
				StreamOption: &persistent.ReadReq_Options_StreamIdentifier{
					StreamIdentifier: &shared.StreamIdentifier{
						StreamName: []byte(streamName),
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
}
