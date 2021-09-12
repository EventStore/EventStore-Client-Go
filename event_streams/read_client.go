package event_streams

import "github.com/EventStore/EventStore-Client-Go/protos/streams2"

type ReadClient interface {
	Recv() (ReadResponse, error)
}

type ReadClientFactory interface {
	Create(protoClient streams2.Streams_ReadClient) ReadClient
}

type ReadClientFactoryImpl struct{}

func (this ReadClientFactoryImpl) Create(protoClient streams2.Streams_ReadClient) ReadClient {
	return newReadClientImpl(protoClient, readResponseAdapterImpl{})
}
