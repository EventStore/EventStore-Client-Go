package client

import (
	"context"
	"fmt"

	"github.com/EventStore/EventStore-Client-Go/client/filtering"
	"github.com/EventStore/EventStore-Client-Go/direction"
	errors2 "github.com/EventStore/EventStore-Client-Go/errors"
	"github.com/EventStore/EventStore-Client-Go/event_streams"
	"github.com/EventStore/EventStore-Client-Go/internal/protoutils"
	"github.com/EventStore/EventStore-Client-Go/messages"
	api "github.com/EventStore/EventStore-Client-Go/protos/streams"
	"github.com/EventStore/EventStore-Client-Go/protos/streams2"
	"github.com/EventStore/EventStore-Client-Go/stream_position"
	stream_revision "github.com/EventStore/EventStore-Client-Go/streamrevision"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func (client *Client) AppendToStream(
	ctx context.Context,
	options event_streams.AppendRequestContentOptions,
	events []event_streams.ProposedEvent,
) (event_streams.WriteResult, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return event_streams.WriteResult{}, err
	}
	eventStreamsClient := client.eventStreamsClientFactory.CreateClient(
		client.grpcClient, streams2.NewStreamsClient(handle.Connection()))

	return eventStreamsClient.AppendToStream(ctx, options, events)
}

// AppendToStream_OLD ...
func (client *Client) AppendToStream_OLD(
	context context.Context,
	streamID string,
	streamRevision stream_revision.StreamRevision,
	events []messages.ProposedEvent,
) (*WriteResult, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}
	streamsClient := api.NewStreamsClient(handle.Connection())
	var headers, trailers metadata.MD

	appendOperation, err := streamsClient.Append(context, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("Could not construct append operation. Reason: %v", err)
	}

	header := protoutils.ToAppendHeader(streamID, streamRevision)

	if err := appendOperation.Send(header); err != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("Could not send append request header. Reason: %v", err)
	}

	for _, event := range events {
		appendRequest := &api.AppendReq{
			Content: &api.AppendReq_ProposedMessage_{
				ProposedMessage: protoutils.ToProposedMessage(event),
			},
		}

		if err = appendOperation.Send(appendRequest); err != nil {
			err = client.grpcClient.HandleError(handle, headers, trailers, err)
			return nil, fmt.Errorf("Could not send append request. Reason: %v", err)
		}
	}

	response, err := appendOperation.CloseAndRecv()
	if err != nil {
		return nil, client.grpcClient.HandleError(handle, headers, trailers, err)
	}

	result := response.GetResult()
	switch result.(type) {
	case *api.AppendResp_Success_:
		{
			success := result.(*api.AppendResp_Success_)
			var streamRevision uint64
			if _, ok := success.Success.GetCurrentRevisionOption().(*api.AppendResp_Success_NoStream); ok {
				streamRevision = 1
			} else {
				streamRevision = success.Success.GetCurrentRevision()
			}

			var commitPosition uint64
			var preparePosition uint64
			if position, ok := success.Success.GetPositionOption().(*api.AppendResp_Success_Position); ok {
				commitPosition = position.Position.CommitPosition
				preparePosition = position.Position.PreparePosition
			} else {
				streamRevision = success.Success.GetCurrentRevision()
			}

			return &WriteResult{
				CommitPosition:      commitPosition,
				PreparePosition:     preparePosition,
				NextExpectedVersion: streamRevision,
			}, nil
		}
	case *api.AppendResp_WrongExpectedVersion_:
		{
			return nil, errors2.ErrWrongExpectedStreamRevision
		}
	}

	return &WriteResult{
		CommitPosition:      0,
		PreparePosition:     0,
		NextExpectedVersion: 1,
	}, nil
}

func (client *Client) DeleteStreamRevision(
	ctx context.Context,
	streamID string,
	streamRevision uint64,
) (event_streams.DeleteResponse, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return event_streams.DeleteResponse{}, err
	}
	eventStreamsClient := client.eventStreamsClientFactory.CreateClient(
		client.grpcClient, streams2.NewStreamsClient(handle.Connection()))

	return eventStreamsClient.DeleteStream(ctx, event_streams.DeleteRequest{
		StreamIdentifier: streamID,
		ExpectedStreamRevision: event_streams.DeleteRequestExpectedStreamRevision{
			Revision: streamRevision,
		},
	})
}

func (client *Client) DeleteStreamRevisionNoStream(
	ctx context.Context,
	streamID string) (event_streams.DeleteResponse, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return event_streams.DeleteResponse{}, err
	}
	eventStreamsClient := client.eventStreamsClientFactory.CreateClient(
		client.grpcClient, streams2.NewStreamsClient(handle.Connection()))

	return eventStreamsClient.DeleteStream(ctx, event_streams.DeleteRequest{
		StreamIdentifier:       streamID,
		ExpectedStreamRevision: event_streams.DeleteRequestExpectedStreamRevisionNoStream{},
	})
}

func (client *Client) DeleteStreamRevisionAny(
	ctx context.Context,
	streamID string) (event_streams.DeleteResponse, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return event_streams.DeleteResponse{}, err
	}
	eventStreamsClient := client.eventStreamsClientFactory.CreateClient(
		client.grpcClient, streams2.NewStreamsClient(handle.Connection()))

	return eventStreamsClient.DeleteStream(ctx, event_streams.DeleteRequest{
		StreamIdentifier:       streamID,
		ExpectedStreamRevision: event_streams.DeleteRequestExpectedStreamRevisionAny{},
	})
}

func (client *Client) DeleteStreamRevisionStreamExists(
	ctx context.Context,
	streamID string) (event_streams.DeleteResponse, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return event_streams.DeleteResponse{}, err
	}
	eventStreamsClient := client.eventStreamsClientFactory.CreateClient(
		client.grpcClient, streams2.NewStreamsClient(handle.Connection()))

	return eventStreamsClient.DeleteStream(ctx, event_streams.DeleteRequest{
		StreamIdentifier:       streamID,
		ExpectedStreamRevision: event_streams.DeleteRequestExpectedStreamRevisionStreamExists{},
	})
}

// DeleteStream_OLD ...
func (client *Client) DeleteStream_OLD(
	context context.Context,
	streamID string,
	streamRevision stream_revision.StreamRevision,
) (*DeleteResult, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}
	streamsClient := api.NewStreamsClient(handle.Connection())
	var headers, trailers metadata.MD
	deleteRequest := protoutils.ToDeleteRequest(streamID, streamRevision)
	deleteResponse, err := streamsClient.Delete(context, deleteRequest, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("Failed to perform delete, details: %v", err)
	}

	return &DeleteResult{Position: protoutils.DeletePositionFromProto(deleteResponse)}, nil
}

func (client *Client) TombstoneStream(
	ctx context.Context,
	streamID string,
	revision uint64) (event_streams.TombstoneResponse, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return event_streams.TombstoneResponse{}, err
	}
	eventStreamsClient := client.eventStreamsClientFactory.CreateClient(
		client.grpcClient, streams2.NewStreamsClient(handle.Connection()))

	return eventStreamsClient.TombstoneStream(ctx, event_streams.TombstoneRequest{
		StreamIdentifier: streamID,
		ExpectedStreamRevision: event_streams.TombstoneRequestExpectedStreamRevision{
			Revision: revision,
		},
	})
}

func (client *Client) TombstoneStreamNoStreamRevision(
	ctx context.Context,
	streamID string) (event_streams.TombstoneResponse, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return event_streams.TombstoneResponse{}, err
	}
	eventStreamsClient := client.eventStreamsClientFactory.CreateClient(
		client.grpcClient, streams2.NewStreamsClient(handle.Connection()))

	return eventStreamsClient.TombstoneStream(ctx, event_streams.TombstoneRequest{
		StreamIdentifier:       streamID,
		ExpectedStreamRevision: event_streams.TombstoneRequestExpectedStreamRevisionNoStream{},
	})
}

func (client *Client) TombstoneStreamAnyRevision(
	ctx context.Context,
	streamID string) (event_streams.TombstoneResponse, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return event_streams.TombstoneResponse{}, err
	}
	eventStreamsClient := client.eventStreamsClientFactory.CreateClient(
		client.grpcClient, streams2.NewStreamsClient(handle.Connection()))

	return eventStreamsClient.TombstoneStream(ctx, event_streams.TombstoneRequest{
		StreamIdentifier:       streamID,
		ExpectedStreamRevision: event_streams.TombstoneRequestExpectedStreamRevisionAny{},
	})
}

func (client *Client) TombstoneStreamRevisionStreamExists(
	ctx context.Context,
	streamID string) (event_streams.TombstoneResponse, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return event_streams.TombstoneResponse{}, err
	}
	eventStreamsClient := client.eventStreamsClientFactory.CreateClient(
		client.grpcClient, streams2.NewStreamsClient(handle.Connection()))

	return eventStreamsClient.TombstoneStream(ctx, event_streams.TombstoneRequest{
		StreamIdentifier:       streamID,
		ExpectedStreamRevision: event_streams.TombstoneRequestExpectedStreamRevisionStreamExists{},
	})
}

// TombstoneStream_OLD Tombstone ...
func (client *Client) TombstoneStream_OLD(
	context context.Context,
	streamID string,
	streamRevision stream_revision.StreamRevision,
) (*DeleteResult, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}
	streamsClient := api.NewStreamsClient(handle.Connection())
	var headers, trailers metadata.MD
	tombstoneRequest := protoutils.ToTombstoneRequest(streamID, streamRevision)
	tombstoneResponse, err := streamsClient.Tombstone(context, tombstoneRequest, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("Failed to perform delete, details: %v", err)
	}

	return &DeleteResult{Position: protoutils.TombstonePositionFromProto(tombstoneResponse)}, nil
}

func (client *Client) ReadStreamEvents(
	ctx context.Context,
	direction event_streams.ReadRequestDirection,
	streamID string,
	revision event_streams.IsReadRequestStreamOptionsStreamRevision,
	count uint64,
	resolveLinks bool) (event_streams.ReadClient, error) {
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

// ReadStreamEvents_OLD ...
func (client *Client) ReadStreamEvents_OLD(
	context context.Context,
	direction direction.Direction,
	streamID string,
	from stream_position.StreamPosition,
	count uint64,
	resolveLinks bool) (*ReadStream, error) {
	readRequest := protoutils.ToReadStreamRequest(streamID, direction, from, count, resolveLinks)
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}
	streamsClient := api.NewStreamsClient(handle.Connection())

	return readInternal(context, client.grpcClient, handle, streamsClient, readRequest)
}

func (client *Client) ReadAllEvents(
	ctx context.Context,
	direction event_streams.ReadRequestDirection,
	position event_streams.IsReadRequestOptionsAllPosition,
	count uint64,
	resolveLinks bool,
) (event_streams.ReadClient, error) {
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

// ReadAllEvents_OLD ...
func (client *Client) ReadAllEvents_OLD(
	context context.Context,
	direction direction.Direction,
	from stream_position.AllStreamPosition,
	count uint64,
	resolveLinks bool,
) (*ReadStream, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}
	streamsClient := api.NewStreamsClient(handle.Connection())
	readRequest := protoutils.ToReadAllRequest(direction, from, count, resolveLinks)
	return readInternal(context, client.grpcClient, handle, streamsClient, readRequest)
}

func (client *Client) SubscribeToStream(
	ctx context.Context,
	streamID string,
	position event_streams.IsSubscribeRequestStreamOptionsStreamRevision,
	resolveLinks bool,
) (event_streams.ReadClient, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}
	eventStreamsClient := client.eventStreamsClientFactory.CreateClient(
		client.grpcClient, streams2.NewStreamsClient(handle.Connection()))

	return eventStreamsClient.SubscribeToStream(ctx, event_streams.SubscribeToStreamRequest{
		StreamOption: event_streams.SubscribeRequestStreamOptions{
			StreamIdentifier: streamID,
			Revision:         position,
		},
		Direction:    event_streams.SubscribeRequestDirectionForward,
		ResolveLinks: resolveLinks,
		Filter:       event_streams.SubscribeRequestNoFilter{},
	})
}

// SubscribeToStream_OLD ...
func (client *Client) SubscribeToStream_OLD(
	ctx context.Context,
	streamID string,
	from stream_position.StreamPosition,
	resolveLinks bool,
) (*Subscription, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}
	var headers, trailers metadata.MD
	streamsClient := api.NewStreamsClient(handle.Connection())
	subscriptionRequest, err := protoutils.ToStreamSubscriptionRequest(streamID, from, resolveLinks, nil)
	if err != nil {
		return nil, fmt.Errorf("Failed to construct subscription. Reason: %v", err)
	}
	ctx, cancel := context.WithCancel(ctx)
	readClient, err := streamsClient.Read(ctx, subscriptionRequest, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		defer cancel()
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("Failed to construct subscription. Reason: %v", err)
	}
	readResult, err := readClient.Recv()
	if err != nil {
		defer cancel()
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("Failed to perform read. Reason: %v", err)
	}
	switch readResult.Content.(type) {
	case *api.ReadResp_Confirmation:
		{
			confirmation := readResult.GetConfirmation()
			return NewSubscription(client, cancel, readClient, confirmation.SubscriptionId), nil
		}
	case *api.ReadResp_StreamNotFound_:
		{
			defer cancel()
			return nil, fmt.Errorf("Failed to initiate subscription because the stream (%s) was not found.", streamID)
		}
	}
	defer cancel()
	return nil, fmt.Errorf("Failed to initiate subscription.")
}

func (client *Client) SubscribeToAll(
	ctx context.Context,
	position event_streams.IsSubscribeRequestOptionsAllPosition,
	resolveLinks bool,
) (event_streams.ReadClient, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}
	eventStreamsClient := client.eventStreamsClientFactory.CreateClient(
		client.grpcClient, streams2.NewStreamsClient(handle.Connection()))

	return eventStreamsClient.SubscribeToStream(ctx, event_streams.SubscribeToStreamRequest{
		StreamOption: event_streams.SubscribeRequestStreamOptionsAll{
			Position: position,
		},
		Direction:    event_streams.SubscribeRequestDirectionForward,
		ResolveLinks: resolveLinks,
		Filter:       event_streams.SubscribeRequestNoFilter{},
	})
}

// SubscribeToAll_OLD ...
func (client *Client) SubscribeToAll_OLD(
	ctx context.Context,
	from stream_position.AllStreamPosition,
	resolveLinks bool,
) (*Subscription, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}
	streamsClient := api.NewStreamsClient(handle.Connection())
	var headers, trailers metadata.MD
	subscriptionRequest, err := protoutils.ToAllSubscriptionRequest(from, resolveLinks, nil)
	ctx, cancel := context.WithCancel(ctx)
	readClient, err := streamsClient.Read(ctx, subscriptionRequest, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		defer cancel()
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("Failed to construct subscription. Reason: %v", err)
	}
	readResult, err := readClient.Recv()
	if err != nil {
		defer cancel()
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("Failed to perform read. Reason: %v", err)
	}
	switch readResult.Content.(type) {
	case *api.ReadResp_Confirmation:
		{
			confirmation := readResult.GetConfirmation()
			return NewSubscription(client, cancel, readClient, confirmation.SubscriptionId), nil
		}
	}
	defer cancel()
	return nil, fmt.Errorf("Failed to initiate subscription.")
}

func (client *Client) SubscribeToAllFiltered(
	ctx context.Context,
	position event_streams.IsSubscribeRequestOptionsAllPosition,
	resolveLinks bool,
	filter event_streams.SubscribeRequestFilter,
) (event_streams.ReadClient, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}
	eventStreamsClient := client.eventStreamsClientFactory.CreateClient(
		client.grpcClient, streams2.NewStreamsClient(handle.Connection()))

	return eventStreamsClient.SubscribeToStream(ctx, event_streams.SubscribeToStreamRequest{
		StreamOption: event_streams.SubscribeRequestStreamOptionsAll{
			Position: position,
		},
		Direction:    event_streams.SubscribeRequestDirectionForward,
		ResolveLinks: resolveLinks,
		Filter:       filter,
	})
}

// SubscribeToAllFiltered_OLD ...
func (client *Client) SubscribeToAllFiltered_OLD(
	ctx context.Context,
	from stream_position.AllStreamPosition,
	resolveLinks bool,
	filterOptions filtering.SubscriptionFilterOptions,
) (*Subscription, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}
	streamsClient := api.NewStreamsClient(handle.Connection())
	subscriptionRequest, err := protoutils.ToAllSubscriptionRequest(from, resolveLinks, &filterOptions)
	if err != nil {
		return nil, fmt.Errorf("Failed to construct subscription. Reason: %v", err)
	}
	var headers, trailers metadata.MD
	ctx, cancel := context.WithCancel(ctx)
	readClient, err := streamsClient.Read(ctx, subscriptionRequest, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		defer cancel()
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("Failed to initiate subscription. Reason: %v", err)
	}
	readResult, err := readClient.Recv()
	if err != nil {
		defer cancel()
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("Failed to read from subscription. Reason: %v", err)
	}
	switch readResult.Content.(type) {
	case *api.ReadResp_Confirmation:
		{
			confirmation := readResult.GetConfirmation()
			return NewSubscription(client, cancel, readClient, confirmation.SubscriptionId), nil
		}
	}
	defer cancel()
	return nil, fmt.Errorf("Failed to initiate subscription.")
}
