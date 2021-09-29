package operations

import "github.com/pivonroll/EventStore-Client-Go/protos/operations"

type StartScavengeRequest struct {
	ThreadCount    int32
	StartFromChunk int32
}

func (request StartScavengeRequest) Build() *operations.StartScavengeReq {
	return &operations.StartScavengeReq{
		Options: &operations.StartScavengeReq_Options{
			ThreadCount:    request.ThreadCount,
			StartFromChunk: request.StartFromChunk,
		},
	}
}
