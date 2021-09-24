package persistent

import (
	"github.com/pivonroll/EventStore-Client-Go/protos/persistent"
	"github.com/pivonroll/EventStore-Client-Go/protos/shared"
)

type CreateOrUpdateStreamRequest struct {
	StreamName string
	GroupName  string
	//	StreamRevision
	//	StreamRevisionStart
	//	StreamRevisionEnd
	Revision isStreamRevision
	Settings CreateOrUpdateRequestSettings
}

func (request CreateOrUpdateStreamRequest) BuildCreateStreamRequest() *persistent.CreateReq {
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

func (request CreateOrUpdateStreamRequest) BuildUpdateStreamRequest() *persistent.UpdateReq {
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
