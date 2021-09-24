package persistent

import (
	"github.com/pivonroll/EventStore-Client-Go/protos/persistent"
	"github.com/pivonroll/EventStore-Client-Go/protos/shared"
)

type isAllPosition interface {
	isCreateRequestAllPosition()
	buildCreateRequestPosition(*persistent.CreateReq_AllOptions)
	buildUpdateRequestPosition(*persistent.UpdateReq_AllOptions)
}

type AllPosition struct {
	Commit  uint64
	Prepare uint64
}

func (c AllPosition) isCreateRequestAllPosition() {}

func (c AllPosition) buildCreateRequestPosition(protoOptions *persistent.CreateReq_AllOptions) {
	protoOptions.AllOption = &persistent.CreateReq_AllOptions_Position{
		Position: &persistent.CreateReq_Position{
			CommitPosition:  c.Commit,
			PreparePosition: c.Prepare,
		},
	}
}

func (c AllPosition) buildUpdateRequestPosition(protoOptions *persistent.UpdateReq_AllOptions) {
	protoOptions.AllOption = &persistent.UpdateReq_AllOptions_Position{
		Position: &persistent.UpdateReq_Position{
			CommitPosition:  c.Commit,
			PreparePosition: c.Prepare,
		},
	}
}

type AllPositionStart struct{}

func (c AllPositionStart) isCreateRequestAllPosition() {
}

func (c AllPositionStart) buildCreateRequestPosition(protoOptions *persistent.CreateReq_AllOptions) {
	protoOptions.AllOption = &persistent.CreateReq_AllOptions_Start{
		Start: &shared.Empty{},
	}
}

func (c AllPositionStart) buildUpdateRequestPosition(protoOptions *persistent.UpdateReq_AllOptions) {
	protoOptions.AllOption = &persistent.UpdateReq_AllOptions_Start{
		Start: &shared.Empty{},
	}
}

type AllPositionEnd struct{}

func (c AllPositionEnd) isCreateRequestAllPosition() {
}

func (c AllPositionEnd) buildCreateRequestPosition(protoOptions *persistent.CreateReq_AllOptions) {
	protoOptions.AllOption = &persistent.CreateReq_AllOptions_End{
		End: &shared.Empty{},
	}
}

func (c AllPositionEnd) buildUpdateRequestPosition(protoOptions *persistent.UpdateReq_AllOptions) {
	protoOptions.AllOption = &persistent.UpdateReq_AllOptions_End{
		End: &shared.Empty{},
	}
}
