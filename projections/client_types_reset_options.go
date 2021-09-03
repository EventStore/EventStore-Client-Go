package projections

import "github.com/EventStore/EventStore-Client-Go/protos/projections"

type ResetOptionsRequest struct {
	name            string
	writeCheckpoint bool
}

func (resetOptionsRequest *ResetOptionsRequest) SetName(name string) *ResetOptionsRequest {
	resetOptionsRequest.name = name
	return resetOptionsRequest
}

func (resetOptionsRequest *ResetOptionsRequest) SetWriteCheckpoint(writeCheckpoint bool) *ResetOptionsRequest {
	resetOptionsRequest.writeCheckpoint = writeCheckpoint
	return resetOptionsRequest
}

func (resetOptionsRequest *ResetOptionsRequest) Build() *projections.ResetReq {
	result := &projections.ResetReq{
		Options: &projections.ResetReq_Options{
			Name:            resetOptionsRequest.name,
			WriteCheckpoint: resetOptionsRequest.writeCheckpoint,
		},
	}

	return result
}
