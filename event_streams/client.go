package event_streams

import (
	"context"

	"github.com/EventStore/EventStore-Client-Go/connection"
)

type Client interface {
	AppendToStream(
		ctx context.Context,
		options AppendRequestContentOptions,
		events []ProposedEvent,
	) (WriteResult, error)

	DeleteStream(
		context context.Context,
		deleteRequest DeleteRequest,
	) (DeleteResponse, error)

	TombstoneStream(
		context context.Context,
		tombstoneRequest TombstoneRequest,
	) (TombstoneResponse, error)

	ReadStreamEvents(
		ctx context.Context,
		readRequest ReadRequest) (ReadClient, error)

	SubscribeToStream(
		ctx context.Context,
		handle connection.ConnectionHandle,
		request SubscribeToStreamRequest,
	) (ReadClient, error)
}
