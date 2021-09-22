package persistent

import (
	"github.com/EventStore/EventStore-Client-Go/protos/persistent"
	"github.com/EventStore/EventStore-Client-Go/protos/shared"
)

type CreateStreamRequest struct {
	StreamName string
	GroupName  string
	//	CreateStreamRevision
	//	CreateStreamRevisionStart
	//	CreateStreamRevisionEnd
	Revision isCreateStreamRevision
	Settings CreateRequestSettings
}

func (request CreateStreamRequest) Build() *persistent.CreateReq {
	streamOption := &persistent.CreateReq_StreamOptions{
		StreamIdentifier: &shared.StreamIdentifier{StreamName: []byte(request.StreamName)},
		RevisionOption:   nil,
	}

	request.Revision.build(streamOption)

	result := &persistent.CreateReq{
		Options: &persistent.CreateReq_Options{
			StreamOption: &persistent.CreateReq_Options_Stream{
				Stream: streamOption,
			},
			StreamIdentifier: &shared.StreamIdentifier{StreamName: []byte(request.StreamName)},
			GroupName:        request.GroupName,
			Settings:         request.Settings.build(),
		},
	}

	return result
}

type isCreateStreamRevision interface {
	isCreateStreamRevision()
	build(*persistent.CreateReq_StreamOptions)
}

type CreateStreamRevision struct {
	Revision uint64
}

func (c CreateStreamRevision) isCreateStreamRevision() {
}

func (c CreateStreamRevision) build(streamOptions *persistent.CreateReq_StreamOptions) {
	streamOptions.RevisionOption = &persistent.CreateReq_StreamOptions_Revision{
		Revision: c.Revision,
	}
}

type CreateStreamRevisionStart struct{}

func (c CreateStreamRevisionStart) isCreateStreamRevision() {
}

func (c CreateStreamRevisionStart) build(streamOptions *persistent.CreateReq_StreamOptions) {
	streamOptions.RevisionOption = &persistent.CreateReq_StreamOptions_Start{
		Start: &shared.Empty{},
	}
}

type CreateStreamRevisionEnd struct{}

func (c CreateStreamRevisionEnd) isCreateStreamRevision() {
}

func (c CreateStreamRevisionEnd) build(streamOptions *persistent.CreateReq_StreamOptions) {
	streamOptions.RevisionOption = &persistent.CreateReq_StreamOptions_End{
		End: &shared.Empty{},
	}
}
