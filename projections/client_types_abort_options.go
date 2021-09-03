package projections

import "github.com/EventStore/EventStore-Client-Go/protos/projections"

type AbortOptionsRequest struct {
	name string
}

func (abortOptionsRequest *AbortOptionsRequest) SetName(name string) *AbortOptionsRequest {
	abortOptionsRequest.name = name
	return abortOptionsRequest
}

func (abortOptionsRequest *AbortOptionsRequest) Build() *projections.DisableReq {
	result := &projections.DisableReq{
		Options: &projections.DisableReq_Options{
			Name:            abortOptionsRequest.name,
			WriteCheckpoint: true,
		},
	}

	return result
}
