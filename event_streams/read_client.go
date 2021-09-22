package event_streams

import (
	"context"

	"google.golang.org/grpc/metadata"

	"github.com/EventStore/EventStore-Client-Go/connection"
	"github.com/EventStore/EventStore-Client-Go/errors"
	"github.com/EventStore/EventStore-Client-Go/protos/streams2"
)

type ReadClient interface {
	Recv() (ReadResponse, errors.Error)
	Close()
}

type ReadClientFactory interface {
	Create(
		grpcClient connection.GrpcClient,
		handle connection.ConnectionHandle,
		headers *metadata.MD,
		trailers *metadata.MD,
		protoClient streams2.Streams_ReadClient,
		cancelFunc context.CancelFunc,
		streamId string) ReadClient
}

type ReadClientFactoryImpl struct{}

func (this ReadClientFactoryImpl) Create(
	grpcClient connection.GrpcClient,
	handle connection.ConnectionHandle,
	headers *metadata.MD,
	trailers *metadata.MD,
	protoClient streams2.Streams_ReadClient,
	cancelFunc context.CancelFunc,
	streamId string) ReadClient {
	return newReadClientImpl(grpcClient,
		handle,
		headers,
		trailers,
		protoClient,
		cancelFunc,
		streamId,
		readResponseAdapterImpl{})
}
