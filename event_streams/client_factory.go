package event_streams

import (
	"github.com/pivonroll/EventStore-Client-Go/connection"
)

// ClientFactory is an interface used to build and return an implementation of the Client interface.
// Existing grpc connection is passed to an implementation of the Client interface.
// This enables all clients to share one grpc connection.
type ClientFactory interface {
	// Create builds and return an implementation of the Client interface.
	Create(grpcClient connection.GrpcClient) Client
}

// ClientFactoryImpl is used to build and return a ClientImpl implementation of Client interface.
type ClientFactoryImpl struct{}

// Create builds and return ClientImpl which implements the Client interface.
func (clientFactory ClientFactoryImpl) Create(
	grpcClient connection.GrpcClient) Client {
	return newClientImpl(grpcClient)
}

func newClientImpl(grpcClient connection.GrpcClient) *ClientImpl {
	return &ClientImpl{
		grpcClient:               grpcClient,
		readClientFactory:        streamReaderFactoryImpl{},
		tombstoneResponseAdapter: tombstoneResponseAdapterImpl{},
		deleteResponseAdapter:    deleteResponseAdapterImpl{},
		appendResponseAdapter:    appendResponseAdapterImpl{},
		readResponseAdapter:      readResponseAdapterImpl{},
		batchResponseAdapter:     batchResponseAdapterImpl{},
	}
}
