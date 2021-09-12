package event_streams

import (
	"github.com/EventStore/EventStore-Client-Go/connection"
	"github.com/EventStore/EventStore-Client-Go/protos/streams2"
	"google.golang.org/grpc/metadata"
)

func newAppendClientImpl(
	protoClient streams2.Streams_AppendClient,
	responseAdapter responseAdapter) *AppendClientImpl {
	return &AppendClientImpl{
		protoClient:     protoClient,
		responseAdapter: responseAdapter,
	}
}

type AppendClientImpl struct {
	grpcClient      connection.GrpcClient
	protoClient     streams2.Streams_AppendClient
	responseAdapter responseAdapter
}

func (this *AppendClientImpl) Send(
	handle connection.ConnectionHandle,
	request AppendRequest) error {
	err := this.protoClient.Send(request.Build())
	if err != nil {
		var headers, trailers metadata.MD
		err = this.grpcClient.HandleError(handle, headers, trailers, err)
		return err
	}

	return nil
}

func (this *AppendClientImpl) CloseAndRecv(
	handle connection.ConnectionHandle) (AppendResponse, error) {
	protoResponse, err := this.protoClient.CloseAndRecv()
	if err != nil {
		var headers, trailers metadata.MD
		err = this.grpcClient.HandleError(handle, headers, trailers, err)
		return AppendResponse{}, err
	}

	result := this.responseAdapter.CreateResponse(protoResponse)
	return result, nil
}
