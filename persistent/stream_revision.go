package persistent

import (
	"github.com/pivonroll/EventStore-Client-Go/protos/persistent"
	"github.com/pivonroll/EventStore-Client-Go/protos/shared"
)

type isStreamRevision interface {
	isCreateStreamRevision()
	buildCreateRequestRevision(*persistent.CreateReq_StreamOptions)
	buildUpdateRequestRevision(*persistent.UpdateReq_StreamOptions)
}

type StreamRevision struct {
	Revision uint64
}

func (c StreamRevision) isCreateStreamRevision() {
}

func (c StreamRevision) buildCreateRequestRevision(
	streamOptions *persistent.CreateReq_StreamOptions) {
	streamOptions.RevisionOption = &persistent.CreateReq_StreamOptions_Revision{
		Revision: c.Revision,
	}
}

func (c StreamRevision) buildUpdateRequestRevision(
	streamOptions *persistent.UpdateReq_StreamOptions) {
	streamOptions.RevisionOption = &persistent.UpdateReq_StreamOptions_Revision{
		Revision: c.Revision,
	}
}

type StreamRevisionStart struct{}

func (c StreamRevisionStart) isCreateStreamRevision() {
}

func (c StreamRevisionStart) buildCreateRequestRevision(
	streamOptions *persistent.CreateReq_StreamOptions) {
	streamOptions.RevisionOption = &persistent.CreateReq_StreamOptions_Start{
		Start: &shared.Empty{},
	}
}

func (c StreamRevisionStart) buildUpdateRequestRevision(
	streamOptions *persistent.UpdateReq_StreamOptions) {
	streamOptions.RevisionOption = &persistent.UpdateReq_StreamOptions_Start{
		Start: &shared.Empty{},
	}
}

type StreamRevisionEnd struct{}

func (c StreamRevisionEnd) isCreateStreamRevision() {
}

func (c StreamRevisionEnd) buildCreateRequestRevision(streamOptions *persistent.CreateReq_StreamOptions) {
	streamOptions.RevisionOption = &persistent.CreateReq_StreamOptions_End{
		End: &shared.Empty{},
	}
}

func (c StreamRevisionEnd) buildUpdateRequestRevision(
	streamOptions *persistent.UpdateReq_StreamOptions) {
	streamOptions.RevisionOption = &persistent.UpdateReq_StreamOptions_End{
		End: &shared.Empty{},
	}
}
