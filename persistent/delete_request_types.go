package persistent

import (
	"github.com/EventStore/EventStore-Client-Go/protos/persistent"
	"github.com/EventStore/EventStore-Client-Go/protos/shared"
)

type DeleteOptions struct {
	StreamName []byte
	GroupName  string
}

func deleteRequestStreamProto(options DeleteOptions) *persistent.DeleteReq {
	return &persistent.DeleteReq{
		Options: &persistent.DeleteReq_Options{
			GroupName: options.GroupName,
			StreamOption: &persistent.DeleteReq_Options_StreamIdentifier{
				StreamIdentifier: &shared.StreamIdentifier{
					StreamName: options.StreamName,
				},
			},
		},
	}
}

func deleteRequestAllOptionsProto(groupName string) *persistent.DeleteReq {
	return &persistent.DeleteReq{
		Options: &persistent.DeleteReq_Options{
			GroupName: groupName,
			StreamOption: &persistent.DeleteReq_Options_All{
				All: &shared.Empty{},
			},
		},
	}
}
