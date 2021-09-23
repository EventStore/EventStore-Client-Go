package event_streams

import (
	"context"

	"github.com/EventStore/EventStore-Client-Go/errors"
)

type Client interface {
	AppendToStream(
		ctx context.Context,
		options AppendRequestContentOptions,
		events []ProposedEvent,
	) (AppendResponse, errors.Error)

	DeleteStream(
		context context.Context,
		deleteRequest DeleteRequest,
	) (DeleteResponse, errors.Error)

	TombstoneStream(
		context context.Context,
		tombstoneRequest TombstoneRequest,
	) (TombstoneResponse, errors.Error)

	ReadStreamEvents(
		ctx context.Context,
		readRequest ReadRequest) ([]ReadResponseEvent, errors.Error)

	ReadStreamEventsReader(
		ctx context.Context,
		readRequest ReadRequest) (ReadClient, errors.Error)

	SubscribeToStream(
		ctx context.Context,
		request SubscribeToStreamRequest,
	) (ReadClient, errors.Error)
}
