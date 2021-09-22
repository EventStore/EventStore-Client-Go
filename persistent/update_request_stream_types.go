package persistent

import (
	"github.com/EventStore/EventStore-Client-Go/protos/persistent"
	"github.com/EventStore/EventStore-Client-Go/protos/shared"
)

type UpdateStreamRequest struct {
	StreamName string
	GroupName  string
	Revision   isStreamRevision
	Settings   CreateRequestSettings
}

func (request UpdateStreamRequest) Build() *persistent.UpdateReq {
	streamOption := &persistent.UpdateReq_StreamOptions{
		StreamIdentifier: &shared.StreamIdentifier{StreamName: []byte(request.StreamName)},
		RevisionOption:   nil,
	}

	request.Revision.buildUpdateRequestRevision(streamOption)

	result := &persistent.UpdateReq{
		Options: &persistent.UpdateReq_Options{
			StreamOption: &persistent.UpdateReq_Options_Stream{
				Stream: streamOption,
			},
			StreamIdentifier: &shared.StreamIdentifier{StreamName: []byte(request.StreamName)},
			GroupName:        request.GroupName,
			Settings:         request.Settings.buildUpdateRequestSettings(),
		},
	}

	return result
}
