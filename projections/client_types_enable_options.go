package projections

import (
	"strings"

	"github.com/pivonroll/EventStore-Client-Go/protos/projections"
)

type EnableOptionsRequest struct {
	name            string
	writeCheckpoint bool
}

func (enableOptionsRequest *EnableOptionsRequest) SetName(name string) *EnableOptionsRequest {
	enableOptionsRequest.name = name
	return enableOptionsRequest
}

func (enableOptionsRequest *EnableOptionsRequest) Build() *projections.EnableReq {
	if strings.TrimSpace(enableOptionsRequest.name) == "" {
		panic("Failed to build EnableOptionsRequest. Trimmed name is an empty string")
	}

	result := &projections.EnableReq{
		Options: &projections.EnableReq_Options{
			Name: enableOptionsRequest.name,
		},
	}

	return result
}
