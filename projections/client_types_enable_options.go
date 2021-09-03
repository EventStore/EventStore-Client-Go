package projections

import "github.com/EventStore/EventStore-Client-Go/protos/projections"

type EnableOptionsRequest struct {
	name            string
	writeCheckpoint bool
}

func (enableOptionsRequest *EnableOptionsRequest) SetName(name string) *EnableOptionsRequest {
	enableOptionsRequest.name = name
	return enableOptionsRequest
}

func (enableOptionsRequest *EnableOptionsRequest) Build() *projections.EnableReq {
	result := &projections.EnableReq{
		Options: &projections.EnableReq_Options{
			Name: enableOptionsRequest.name,
		},
	}

	return result
}
