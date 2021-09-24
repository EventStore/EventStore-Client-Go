package projections

import (
	"strings"

	"github.com/pivonroll/EventStore-Client-Go/protos/projections"
)

type AbortOptionsRequest struct {
	name string
}

func (abortOptionsRequest *AbortOptionsRequest) SetName(name string) *AbortOptionsRequest {
	abortOptionsRequest.name = name
	return abortOptionsRequest
}

func (abortOptionsRequest *AbortOptionsRequest) Build() *projections.DisableReq {
	if strings.TrimSpace(abortOptionsRequest.name) == "" {
		panic("Failed to build AbortOptionsRequest. Trimmed name is an empty string")
	}

	result := &projections.DisableReq{
		Options: &projections.DisableReq_Options{
			Name:            abortOptionsRequest.name,
			WriteCheckpoint: true,
		},
	}

	return result
}
