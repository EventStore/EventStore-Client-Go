package event_streams

import (
	"github.com/pivonroll/EventStore-Client-Go/connection"
)

type ClientFactory interface {
	Create(grpcClient connection.GrpcClient) Client
}

type ClientFactoryImpl struct{}

func (clientFactory ClientFactoryImpl) Create(
	grpcClient connection.GrpcClient) Client {
	return newClientImpl(grpcClient)
}

func newClientImpl(grpcClient connection.GrpcClient) *ClientImpl {
	return &ClientImpl{
		grpcClient:               grpcClient,
		readClientFactory:        StreamReaderFactoryImpl{},
		tombstoneResponseAdapter: tombstoneResponseAdapterImpl{},
		deleteResponseAdapter:    deleteResponseAdapterImpl{},
		appendResponseAdapter:    appendResponseAdapterImpl{},
		readResponseAdapter:      readResponseAdapterImpl{},
	}
}
