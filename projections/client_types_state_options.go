package projections

import (
	"strings"

	"github.com/EventStore/EventStore-Client-Go/protos/projections"
)

type StateOptionsRequest struct {
	name      string
	partition string
}

func (stateOptionsRequest *StateOptionsRequest) SetName(name string) *StateOptionsRequest {
	stateOptionsRequest.name = name
	return stateOptionsRequest
}

func (stateOptionsRequest *StateOptionsRequest) SetPartition(partition string) *StateOptionsRequest {
	stateOptionsRequest.partition = partition
	return stateOptionsRequest
}

func (stateOptionsRequest *StateOptionsRequest) Build() *projections.StateReq {
	if strings.TrimSpace(stateOptionsRequest.name) == "" {
		panic("Failed to build StateOptionsRequest. Trimmed name is an empty string")
	}

	result := &projections.StateReq{
		Options: &projections.StateReq_Options{
			Name:      stateOptionsRequest.name,
			Partition: stateOptionsRequest.partition,
		},
	}

	return result
}
