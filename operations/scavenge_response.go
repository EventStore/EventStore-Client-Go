package operations

import "github.com/pivonroll/EventStore-Client-Go/protos/operations"

type ScavengeResponse struct {
	ScavengeId string
	Status     ScavengeStatus
}

type ScavengeStatus int32

const (
	ScavengeStatus_Started    ScavengeStatus = 0
	ScavengeStatus_InProgress ScavengeStatus = 1
	ScavengeStatus_Stopped    ScavengeStatus = 2
)

type scavengeResponseAdapter interface {
	create(protoResponse *operations.ScavengeResp) ScavengeResponse
}

type scavengeResponseAdapterImpl struct{}

func (adapter scavengeResponseAdapterImpl) create(
	protoResponse *operations.ScavengeResp) ScavengeResponse {
	result := ScavengeResponse{
		ScavengeId: protoResponse.ScavengeId,
		Status:     ScavengeStatus(protoResponse.ScavengeResult),
	}
	return result
}
