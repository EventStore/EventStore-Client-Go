package projections

import (
	"strings"

	"github.com/pivonroll/EventStore-Client-Go/protos/projections"
)

type DisableOptionsRequest struct {
	name string
}

func (disableOptionsRequest *DisableOptionsRequest) SetName(name string) *DisableOptionsRequest {
	disableOptionsRequest.name = name
	return disableOptionsRequest
}

func (disableOptionsRequest *DisableOptionsRequest) Build() *projections.DisableReq {
	if strings.TrimSpace(disableOptionsRequest.name) == "" {
		panic("Failed to build DisableOptionsRequest. Trimmed name is an empty string")
	}

	result := &projections.DisableReq{
		Options: &projections.DisableReq_Options{
			Name:            disableOptionsRequest.name,
			WriteCheckpoint: false,
		},
	}

	return result
}
