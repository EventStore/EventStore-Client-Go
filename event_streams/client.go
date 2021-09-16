package event_streams

import "context"

type Client interface {
	AppendToStream(
		ctx context.Context,
		options AppendRequestContentOptions,
		events []ProposedEvent,
	) (WriteResult, error)
}
