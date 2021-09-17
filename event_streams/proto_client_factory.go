package event_streams

import (
	"github.com/EventStore/EventStore-Client-Go/connection"
	streamsProto "github.com/EventStore/EventStore-Client-Go/protos/streams2"
)

type ProtoClientFactory interface {
	CreateClient(grpcClient connection.GrpcClient, client streamsProto.StreamsClient) ProtoClient
}

type ProtoClientFactoryImpl struct{}

func (clientFactory ProtoClientFactoryImpl) CreateClient(
	grpcClient connection.GrpcClient,
	client streamsProto.StreamsClient) ProtoClient {
	return newProtoClientImpl(grpcClient, client)
}

func newProtoClientImpl(
	grpcClient connection.GrpcClient,
	grpcStreamsClient streamsProto.StreamsClient) *ProtoClientImpl {
	return &ProtoClientImpl{
		grpcClient:               grpcClient,
		grpcStreamsClient:        grpcStreamsClient,
		readClientFactory:        nil,
		appendClientFactory:      nil,
		batchAppendClientFactory: nil,
		deleteResponseAdapter:    nil,
		tombstoneResponseAdapter: nil,
	}
}
