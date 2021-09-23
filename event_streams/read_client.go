package event_streams

import (
	"context"

	"github.com/EventStore/EventStore-Client-Go/errors"
	"github.com/EventStore/EventStore-Client-Go/protos/streams2"
)

type ReadClient interface {
	ReadOne() (ReadResponse, errors.Error)
	Close()
}

type ReadClientFactory interface {
	Create(
		protoClient streams2.Streams_ReadClient,
		cancelFunc context.CancelFunc) ReadClient
}

type ReadClientFactoryImpl struct{}

func (this ReadClientFactoryImpl) Create(
	protoClient streams2.Streams_ReadClient,
	cancelFunc context.CancelFunc) ReadClient {
	return newReadClientImpl(
		protoClient,
		cancelFunc,
		readResponseAdapterImpl{})
}
