package event_streams

import (
	"github.com/EventStore/EventStore-Client-Go/connection"
	streamsProto "github.com/EventStore/EventStore-Client-Go/protos/streams2"
)

type ClientFactory interface {
	CreateClient(grpcClient connection.GrpcClient, client streamsProto.StreamsClient) Client
}

type ClientFactoryImpl struct{}

func (clientFactory ClientFactoryImpl) CreateClient(
	grpcClient connection.GrpcClient,
	client streamsProto.StreamsClient) Client {
	return newClientImpl(grpcClient, client)
}

func newClientImpl(client connection.GrpcClient, client2 streamsProto.StreamsClient) Client {
	return nil
}
