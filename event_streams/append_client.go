package event_streams

import (
	"github.com/EventStore/EventStore-Client-Go/connection"
	"github.com/EventStore/EventStore-Client-Go/protos/streams2"
)

type Appender interface {
	Send(connection.ConnectionHandle, AppendRequest) error
	CloseAndRecv(connection.ConnectionHandle) (AppendResponse, error)
}

type AppendClientFactory interface {
	Create(protoClient streams2.Streams_AppendClient) Appender
}

type AppendClientFactoryImpl struct{}

func (this AppendClientFactoryImpl) Create(
	protoClient streams2.Streams_AppendClient) Appender {
	return newAppendClientImpl(protoClient, responseAdapterImpl{})
}
