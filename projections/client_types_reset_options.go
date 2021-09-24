package projections

import (
	"strings"

	"github.com/pivonroll/EventStore-Client-Go/protos/projections"
)

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
	if strings.TrimSpace(resetOptionsRequest.name) == "" {
		panic("Failed to build ResetOptionsRequest. Trimmed name is an empty string")
	}

	result := &projections.ResetReq{
		Options: &projections.ResetReq_Options{
			Name:            resetOptionsRequest.name,
			WriteCheckpoint: resetOptionsRequest.writeCheckpoint,
		},
	}

	return result
}
