package event_streams

import (
	"context"

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
		protoClient streams2.Streams_ReadClient,
		cancelFunc context.CancelFunc,
		streamId string) ReadClient
}

type ReadClientFactoryImpl struct{}

func (this ReadClientFactoryImpl) Create(
	grpcClient connection.GrpcClient,
	protoClient streams2.Streams_ReadClient,
	cancelFunc context.CancelFunc,
	streamId string) ReadClient {
	return newReadClientImpl(grpcClient, protoClient, cancelFunc, streamId, readResponseAdapterImpl{})
}
