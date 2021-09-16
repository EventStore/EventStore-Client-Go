package event_streams

import (
	"context"

	"github.com/EventStore/EventStore-Client-Go/protos/streams2"
)

type ReadClient interface {
	Recv() (ReadResponse, error)
}

type ReadClientFactory interface {
	Create(protoClient streams2.Streams_ReadClient,
		cancelFunc context.CancelFunc,
		streamId string) ReadClient
}

type ReadClientFactoryImpl struct{}

func (this ReadClientFactoryImpl) Create(protoClient streams2.Streams_ReadClient,
	cancelFunc context.CancelFunc,
	streamId string) ReadClient {
	return newReadClientImpl(protoClient, readResponseAdapterImpl{}, streamId)
}
