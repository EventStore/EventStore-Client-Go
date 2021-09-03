package projections

import "github.com/EventStore/EventStore-Client-Go/protos/projections"

type DeleteOptionsRequest struct {
	name                   string
	deleteEmittedStreams   bool
	deleteStateStream      bool
	deleteCheckpointStream bool
}

func (deleteOptions *DeleteOptionsRequest) SetName(name string) *DeleteOptionsRequest {
	deleteOptions.name = name
	return deleteOptions
}

func (deleteOptions *DeleteOptionsRequest) SetDeleteEmittedStreams(delete bool) *DeleteOptionsRequest {
	deleteOptions.deleteEmittedStreams = delete
	return deleteOptions
}

func (deleteOptions *DeleteOptionsRequest) SetDeleteStateStream(delete bool) *DeleteOptionsRequest {
	deleteOptions.deleteStateStream = delete
	return deleteOptions
}

func (deleteOptions *DeleteOptionsRequest) SetDeleteCheckpointStream(delete bool) *DeleteOptionsRequest {
	deleteOptions.deleteCheckpointStream = delete
	return deleteOptions
}

func (deleteOptions *DeleteOptionsRequest) Build() *projections.DeleteReq {
	result := &projections.DeleteReq{
		Options: &projections.DeleteReq_Options{
			Name:                   deleteOptions.name,
			DeleteEmittedStreams:   deleteOptions.deleteEmittedStreams,
			DeleteStateStream:      deleteOptions.deleteStateStream,
			DeleteCheckpointStream: deleteOptions.deleteCheckpointStream,
		},
	}

	return result
}
