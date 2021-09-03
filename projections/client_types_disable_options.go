package projections

import "github.com/EventStore/EventStore-Client-Go/protos/projections"

type DisableOptionsRequest struct {
	name string
}

func (disableOptionsRequest *DisableOptionsRequest) SetName(name string) *DisableOptionsRequest {
	disableOptionsRequest.name = name
	return disableOptionsRequest
}

func (disableOptionsRequest *DisableOptionsRequest) Build() *projections.DisableReq {
	result := &projections.DisableReq{
		Options: &projections.DisableReq_Options{
			Name:            disableOptionsRequest.name,
			WriteCheckpoint: false,
		},
	}

	return result
}
