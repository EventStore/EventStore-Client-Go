package persistent

import (
	"github.com/pivonroll/EventStore-Client-Go/protos/persistent"
	"github.com/pivonroll/EventStore-Client-Go/protos/shared"
)

type DeleteRequest struct {
	StreamName string
	GroupName  string
}

func (options DeleteRequest) Build() *persistent.DeleteReq {
	return &persistent.DeleteReq{
		Options: &persistent.DeleteReq_Options{
			GroupName: options.GroupName,
			StreamOption: &persistent.DeleteReq_Options_StreamIdentifier{
				StreamIdentifier: &shared.StreamIdentifier{
					StreamName: []byte(options.StreamName),
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
