package persistent

import (
	"github.com/EventStore/EventStore-Client-Go/protos/persistent"
	"github.com/EventStore/EventStore-Client-Go/protos/shared"
)

type CreateStreamRequest struct {
	StreamName string
	GroupName  string
	//	StreamRevision
	//	StreamRevisionStart
	//	StreamRevisionEnd
	Revision isStreamRevision
	Settings CreateRequestSettings
}

func (request CreateStreamRequest) Build() *persistent.CreateReq {
	streamOption := &persistent.CreateReq_StreamOptions{
		StreamIdentifier: &shared.StreamIdentifier{StreamName: []byte(request.StreamName)},
		RevisionOption:   nil,
	}

	request.Revision.buildCreateRequestRevision(streamOption)

	result := &persistent.CreateReq{
		Options: &persistent.CreateReq_Options{
			StreamOption: &persistent.CreateReq_Options_Stream{
				Stream: streamOption,
			},
			StreamIdentifier: &shared.StreamIdentifier{StreamName: []byte(request.StreamName)},
			GroupName:        request.GroupName,
			Settings:         request.Settings.buildCreateRequestSettings(),
		},
	}

	return result
}
