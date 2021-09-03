package projections

import (
	"strings"

	"github.com/EventStore/EventStore-Client-Go/protos/projections"
)

type ResultOptionsRequest struct {
	name      string
	partition string
}

func (resultOptionsRequest *ResultOptionsRequest) SetName(name string) *ResultOptionsRequest {
	resultOptionsRequest.name = name
	return resultOptionsRequest
}

func (resultOptionsRequest *ResultOptionsRequest) SetPartition(partition string) *ResultOptionsRequest {
	resultOptionsRequest.partition = partition
	return resultOptionsRequest
}

func (resultOptionsRequest *ResultOptionsRequest) Build() *projections.ResultReq {
	if strings.TrimSpace(resultOptionsRequest.name) == "" {
		panic("Failed to build ResultOptionsRequest. Trimmed name is an empty string")
	}

	result := &projections.ResultReq{
		Options: &projections.ResultReq_Options{
			Name:      resultOptionsRequest.name,
			Partition: resultOptionsRequest.partition,
		},
	}

	return result
}
