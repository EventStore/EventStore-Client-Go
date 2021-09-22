package client

import (
	"context"

	"github.com/EventStore/EventStore-Client-Go/errors"
	"github.com/EventStore/EventStore-Client-Go/event_streams"
	"github.com/EventStore/EventStore-Client-Go/protos/streams2"
)

func (client *Client) AppendToStream(
	ctx context.Context,
	streamID string,
	expectedStreamRevision event_streams.IsAppendRequestExpectedStreamRevision,
	events []event_streams.ProposedEvent,
) (event_streams.AppendResponse, errors.Error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return event_streams.AppendResponse{}, err
	}
	eventStreamsClient := client.eventStreamsClientFactory.CreateClient(
		client.grpcClient, streams2.NewStreamsClient(handle.Connection()))

	return eventStreamsClient.AppendToStream(ctx, event_streams.AppendRequestContentOptions{
		StreamIdentifier:       streamID,
		ExpectedStreamRevision: expectedStreamRevision,
	}, events)
}

func (client *Client) DeleteStream(
	ctx context.Context,
	streamID string,
	revision event_streams.IsDeleteRequestExpectedStreamRevision) (event_streams.DeleteResponse, errors.Error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return event_streams.DeleteResponse{}, err
	}
	eventStreamsClient := client.eventStreamsClientFactory.CreateClient(
		client.grpcClient, streams2.NewStreamsClient(handle.Connection()))

	return eventStreamsClient.DeleteStream(ctx, event_streams.DeleteRequest{
		StreamIdentifier:       streamID,
		ExpectedStreamRevision: revision,
	})
}

func (client *Client) TombstoneStream(
	ctx context.Context,
	streamID string,
	revision event_streams.IsTombstoneRequestExpectedStreamRevision) (event_streams.TombstoneResponse, errors.Error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return event_streams.TombstoneResponse{}, err
	}
	eventStreamsClient := client.eventStreamsClientFactory.CreateClient(
		client.grpcClient, streams2.NewStreamsClient(handle.Connection()))

	return eventStreamsClient.TombstoneStream(ctx, event_streams.TombstoneRequest{
		StreamIdentifier:       streamID,
		ExpectedStreamRevision: revision,
	})
}

func (client *Client) ReadStreamEvents(
	ctx context.Context,
	streamID string,
	direction event_streams.ReadRequestDirection,
	revision event_streams.IsReadRequestStreamOptionsStreamRevision,
	count uint64,
	resolveLinks bool) (event_streams.ReadResponseEventList, errors.Error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}
	eventStreamsClient := client.eventStreamsClientFactory.CreateClient(
		client.grpcClient, streams2.NewStreamsClient(handle.Connection()))

	return eventStreamsClient.ReadStreamEvents(ctx, event_streams.ReadRequest{
		StreamOption: event_streams.ReadRequestStreamOptions{
			StreamIdentifier: streamID,
			Revision:         revision,
		},
		Direction:    direction,
		ResolveLinks: resolveLinks,
		Count:        count,
		Filter:       event_streams.ReadRequestNoFilter{},
	})
}

func (client *Client) GetStreamReader(
	ctx context.Context,
	streamID string,
	direction event_streams.ReadRequestDirection,
	revision event_streams.IsReadRequestStreamOptionsStreamRevision,
	count uint64,
	resolveLinks bool) (event_streams.ReadClient, errors.Error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}
	eventStreamsClient := client.eventStreamsClientFactory.CreateClient(
		client.grpcClient, streams2.NewStreamsClient(handle.Connection()))

	return eventStreamsClient.ReadStreamEventsReader(ctx, event_streams.ReadRequest{
		StreamOption: event_streams.ReadRequestStreamOptions{
			StreamIdentifier: streamID,
			Revision:         revision,
		},
		Direction:    direction,
		ResolveLinks: resolveLinks,
		Count:        count,
		Filter:       event_streams.ReadRequestNoFilter{},
	})
}

func (client *Client) ReadAllEvents(
	ctx context.Context,
	direction event_streams.ReadRequestDirection,
	position event_streams.IsReadRequestOptionsAllPosition,
	count uint64,
	resolveLinks bool,
) (event_streams.ReadResponseEventList, errors.Error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}
	eventStreamsClient := client.eventStreamsClientFactory.CreateClient(
		client.grpcClient, streams2.NewStreamsClient(handle.Connection()))

	return eventStreamsClient.ReadStreamEvents(ctx, event_streams.ReadRequest{
		StreamOption: event_streams.ReadRequestStreamOptionsAll{
			Position: position,
		},
		Direction:    direction,
		ResolveLinks: resolveLinks,
		Count:        count,
		Filter:       event_streams.ReadRequestNoFilter{},
	})
}

func (client *Client) GetAllEventsReader(
	ctx context.Context,
	direction event_streams.ReadRequestDirection,
	position event_streams.IsReadRequestOptionsAllPosition,
	count uint64,
	resolveLinks bool,
) (event_streams.ReadClient, errors.Error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}
	eventStreamsClient := client.eventStreamsClientFactory.CreateClient(
		client.grpcClient, streams2.NewStreamsClient(handle.Connection()))

	return eventStreamsClient.ReadStreamEventsReader(ctx, event_streams.ReadRequest{
		StreamOption: event_streams.ReadRequestStreamOptionsAll{
			Position: position,
		},
		Direction:    direction,
		ResolveLinks: resolveLinks,
		Count:        count,
		Filter:       event_streams.ReadRequestNoFilter{},
	})
}

func (client *Client) SubscribeToStream(
	ctx context.Context,
	streamID string,
	revision event_streams.IsSubscribeRequestStreamOptionsStreamRevision,
	resolveLinks bool,
) (event_streams.ReadClient, errors.Error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}
	eventStreamsClient := client.eventStreamsClientFactory.CreateClient(
		client.grpcClient, streams2.NewStreamsClient(handle.Connection()))

	return eventStreamsClient.SubscribeToStream(ctx, handle, event_streams.SubscribeToStreamRequest{
		StreamOption: event_streams.SubscribeRequestStreamOptions{
			StreamIdentifier: streamID,
			Revision:         revision,
		},
		Direction:    event_streams.SubscribeRequestDirectionForward,
		ResolveLinks: resolveLinks,
		Filter:       event_streams.SubscribeRequestNoFilter{},
	})
}

func (client *Client) SubscribeToAll(
	ctx context.Context,
	position event_streams.IsSubscribeRequestOptionsAllPosition,
	resolveLinks bool,
) (event_streams.ReadClient, errors.Error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}
	eventStreamsClient := client.eventStreamsClientFactory.CreateClient(
		client.grpcClient, streams2.NewStreamsClient(handle.Connection()))

	return eventStreamsClient.SubscribeToStream(ctx, handle, event_streams.SubscribeToStreamRequest{
		StreamOption: event_streams.SubscribeRequestStreamOptionsAll{
			Position: position,
		},
		Direction:    event_streams.SubscribeRequestDirectionForward,
		ResolveLinks: resolveLinks,
		Filter:       event_streams.SubscribeRequestNoFilter{},
	})
}

func (client *Client) SubscribeToAllFiltered(
	ctx context.Context,
	position event_streams.IsSubscribeRequestOptionsAllPosition,
	resolveLinks bool,
	filter event_streams.SubscribeRequestFilter,
) (event_streams.ReadClient, errors.Error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}
	eventStreamsClient := client.eventStreamsClientFactory.CreateClient(
		client.grpcClient, streams2.NewStreamsClient(handle.Connection()))

	return eventStreamsClient.SubscribeToStream(ctx, handle, event_streams.SubscribeToStreamRequest{
		StreamOption: event_streams.SubscribeRequestStreamOptionsAll{
			Position: position,
		},
		Direction:    event_streams.SubscribeRequestDirectionForward,
		ResolveLinks: resolveLinks,
		Filter:       filter,
	})
}

func (client *Client) SetStreamMetadata(
	ctx context.Context,
	streamID string,
	expectedStreamRevision event_streams.IsAppendRequestExpectedStreamRevision,
	metadata event_streams.StreamMetadata) (event_streams.AppendResponse, errors.Error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return event_streams.AppendResponse{}, err
	}
	eventStreamsClient := client.eventStreamsClientFactory.CreateClient(
		client.grpcClient, streams2.NewStreamsClient(handle.Connection()))

	streamMetadataEvent := event_streams.NewMetadataEvent(metadata)

	return eventStreamsClient.AppendToStream(ctx, event_streams.AppendRequestContentOptions{
		StreamIdentifier:       event_streams.GetMetaStreamOf(streamID),
		ExpectedStreamRevision: expectedStreamRevision,
	}, []event_streams.ProposedEvent{streamMetadataEvent})
}

func (client *Client) GetStreamMetadata(
	ctx context.Context,
	streamID string) (event_streams.StreamMetadataResult, errors.Error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return event_streams.StreamMetadataNone{}, err
	}
	eventStreamsClient := client.eventStreamsClientFactory.CreateClient(
		client.grpcClient, streams2.NewStreamsClient(handle.Connection()))

	events, err := eventStreamsClient.ReadStreamEvents(ctx, event_streams.ReadRequest{
		StreamOption: event_streams.ReadRequestStreamOptions{
			StreamIdentifier: event_streams.GetMetaStreamOf(streamID),
			Revision:         event_streams.ReadRequestOptionsStreamRevisionEnd{},
		},
		Direction:    event_streams.ReadRequestDirectionBackward,
		ResolveLinks: false,
		Count:        1,
		Filter:       event_streams.ReadRequestNoFilter{},
	})
	if err != nil {
		if err.Code() == errors.StreamNotFoundErr {
			return event_streams.StreamMetadataNone{}, nil
		}
		return event_streams.StreamMetadataNone{}, err
	}

	if len(events) == 0 {
		return event_streams.StreamMetadataNone{}, nil
	}

	return event_streams.NewStreamMetadataResultImpl(streamID, events[0]), nil
}
