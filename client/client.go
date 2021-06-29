package client

import (
	"context"
	"fmt"
	"io"

	"github.com/EventStore/EventStore-Client-Go/connection"
	"github.com/EventStore/EventStore-Client-Go/persistent"
	persistentProto "github.com/EventStore/EventStore-Client-Go/protos/persistent"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/EventStore/EventStore-Client-Go/client/filtering"
	"github.com/EventStore/EventStore-Client-Go/direction"
	"github.com/EventStore/EventStore-Client-Go/errors"
	"github.com/EventStore/EventStore-Client-Go/internal/protoutils"
	"github.com/EventStore/EventStore-Client-Go/messages"
	"github.com/EventStore/EventStore-Client-Go/position"
	api "github.com/EventStore/EventStore-Client-Go/protos/streams"
	stream_revision "github.com/EventStore/EventStore-Client-Go/streamrevision"
	"github.com/EventStore/EventStore-Client-Go/subscription"
)

type Configuration = connection.Configuration

func ParseConnectionString(str string) (*connection.Configuration, error) {
	return connection.ParseConnectionString(str)
}

// Client ...
type Client struct {
	grpcClient connection.GrpcClient
	Config     *connection.Configuration
}

// NewClient ...
func NewClient(configuration *connection.Configuration) (*Client, error) {
	grpcClient := connection.NewGrpcClient(*configuration)
	return &Client{
		grpcClient: grpcClient,
		Config:     configuration,
	}, nil
}

// Close ...
func (client *Client) Close() error {
	client.grpcClient.Close()
	return nil
}

// AppendToStream ...
func (client *Client) AppendToStream(
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
			return nil, errors.ErrWrongExpectedStreamRevision
		}
	}

	return &WriteResult{
		CommitPosition:      0,
		PreparePosition:     0,
		NextExpectedVersion: 1,
	}, nil
}

// DeleteStream ...
func (client *Client) DeleteStream(
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

// Tombstone ...
func (client *Client) TombstoneStream(
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

// ReadStreamEvents ...
func (client *Client) ReadStreamEvents(
	context context.Context,
	direction direction.Direction,
	streamID string,
	from uint64,
	count uint64,
	resolveLinks bool) ([]messages.RecordedEvent, error) {
	readRequest := protoutils.ToReadStreamRequest(streamID, direction, from, count, resolveLinks)
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}
	streamsClient := api.NewStreamsClient(handle.Connection())

	return readInternal(context, client.grpcClient, handle, streamsClient, readRequest, count)
}

// ReadAllEvents ...
func (client *Client) ReadAllEvents(
	context context.Context,
	direction direction.Direction,
	from position.Position,
	count uint64,
	resolveLinks bool,
) ([]messages.RecordedEvent, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}
	streamsClient := api.NewStreamsClient(handle.Connection())
	readRequest := protoutils.ToReadAllRequest(direction, from, count, resolveLinks)
	return readInternal(context, client.grpcClient, handle, streamsClient, readRequest, count)
}

// SubscribeToStream ...
func (client *Client) SubscribeToStream(
	context context.Context,
	streamID string,
	from uint64,
	resolveLinks bool,
	eventAppeared func(messages.RecordedEvent),
	checkpointReached func(position.Position),
	subscriptionDropped func(reason string),
) (*subscription.Subscription, error) {
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
	readClient, err := streamsClient.Read(context, subscriptionRequest, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("Failed to construct subscription. Reason: %v", err)
	}
	readResult, err := readClient.Recv()
	if err != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("Failed to perform read. Reason: %v", err)
	}
	switch readResult.Content.(type) {
	case *api.ReadResp_Confirmation:
		{
			confirmation := readResult.GetConfirmation()
			return subscription.NewSubscription(readClient, confirmation.SubscriptionId, eventAppeared,
				checkpointReached, subscriptionDropped), nil
		}
	case *api.ReadResp_StreamNotFound_:
		{
			return nil, fmt.Errorf("Failed to initiate subscription because the stream (%s) was not found.", streamID)
		}
	}
	return nil, fmt.Errorf("Failed to initiate subscription.")
}

// SubscribeToAll ...
func (client *Client) SubscribeToAll(
	context context.Context,
	from position.Position,
	resolveLinks bool,
	eventAppeared func(messages.RecordedEvent),
	checkpointReached func(position.Position),
	subscriptionDropped func(reason string),
) (*subscription.Subscription, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}
	streamsClient := api.NewStreamsClient(handle.Connection())
	var headers, trailers metadata.MD
	subscriptionRequest, err := protoutils.ToAllSubscriptionRequest(from, resolveLinks, nil)
	readClient, err := streamsClient.Read(context, subscriptionRequest, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("Failed to construct subscription. Reason: %v", err)
	}
	readResult, err := readClient.Recv()
	if err != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("Failed to perform read. Reason: %v", err)
	}
	switch readResult.Content.(type) {
	case *api.ReadResp_Confirmation:
		{
			confirmation := readResult.GetConfirmation()
			return subscription.NewSubscription(readClient, confirmation.SubscriptionId,
				eventAppeared, checkpointReached, subscriptionDropped), nil
		}
	}
	return nil, fmt.Errorf("Failed to initiate subscription.")
}

// SubscribeToAllFiltered ...
func (client *Client) SubscribeToAllFiltered(
	context context.Context,
	from position.Position,
	resolveLinks bool,
	filterOptions filtering.SubscriptionFilterOptions,
	eventAppeared func(messages.RecordedEvent),
	checkpointReached func(position.Position),
	subscriptionDropped func(reason string),
) (*subscription.Subscription, error) {
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
	readClient, err := streamsClient.Read(context, subscriptionRequest, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("Failed to initiate subscription. Reason: %v", err)
	}
	readResult, err := readClient.Recv()
	if err != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("Failed to read from subscription. Reason: %v", err)
	}
	switch readResult.Content.(type) {
	case *api.ReadResp_Confirmation:
		{
			confirmation := readResult.GetConfirmation()
			return subscription.NewSubscription(readClient, confirmation.SubscriptionId, eventAppeared,
				checkpointReached, subscriptionDropped), nil
		}
	}
	return nil, fmt.Errorf("Failed to initiate subscription.")
}

// ConnectToPersistentSubscription ...
func (client *Client) ConnectToPersistentSubscription(
	ctx context.Context,
	bufferSize int32,
	groupName string,
	streamName []byte,
) (persistent.SyncReadConnection, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}
	persistentSubscriptionClient := persistent.NewClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

	return persistentSubscriptionClient.SubscribeToStreamSync(
		ctx,
		handle,
		bufferSize,
		groupName,
		streamName,
	)
}

func (client *Client) CreatePersistentSubscription(
	ctx context.Context,
	streamConfig persistent.SubscriptionStreamConfig,
) error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}
	persistentSubscriptionClient := persistent.NewClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

	return persistentSubscriptionClient.CreateStreamSubscription(ctx, handle, streamConfig)
}

func (client *Client) CreatePersistentSubscriptionAll(
	ctx context.Context,
	allOptions persistent.SubscriptionAllOptionConfig,
) error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}
	persistentSubscriptionClient := persistent.NewClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

	return persistentSubscriptionClient.CreateAllSubscription(ctx, handle, allOptions)
}

func (client *Client) UpdatePersistentStreamSubscription(
	ctx context.Context,
	streamConfig persistent.SubscriptionStreamConfig,
) error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}
	persistentSubscriptionClient := persistent.NewClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

	return persistentSubscriptionClient.UpdateStreamSubscription(ctx, handle, streamConfig)
}

func (client *Client) UpdatePersistentSubscriptionAll(
	ctx context.Context,
	allOptions persistent.SubscriptionUpdateAllOptionConfig,
) error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}
	persistentSubscriptionClient := persistent.NewClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

	return persistentSubscriptionClient.UpdateAllSubscription(ctx, handle, allOptions)
}

func (client *Client) DeletePersistentSubscription(
	ctx context.Context,
	deleteOptions persistent.DeleteOptions,
) error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}
	persistentSubscriptionClient := persistent.NewClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

	return persistentSubscriptionClient.DeleteStreamSubscription(ctx, handle, deleteOptions)
}

func (client *Client) DeletePersistentSubscriptionAll(
	ctx context.Context,
	groupName string,
) error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}
	persistentSubscriptionClient := persistent.NewClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

	return persistentSubscriptionClient.DeleteAllSubscription(ctx, handle, groupName)
}

func readInternal(
	context context.Context,
	client connection.GrpcClient,
	handle connection.ConnectionHandle,
	streamsClient api.StreamsClient,
	readRequest *api.ReadReq,
	limit uint64,
) ([]messages.RecordedEvent, error) {
	var headers, trailers metadata.MD
	result, err := streamsClient.Read(context, readRequest, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.HandleError(handle, headers, trailers, err)
		return []messages.RecordedEvent{}, fmt.Errorf("Failed to construct read client. Reason: %v", err)
	}

	events := []messages.RecordedEvent{}
	for {
		readResult, err := result.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			err = client.HandleError(handle, headers, trailers, err)
			return nil, fmt.Errorf("Failed to perform read. Reason: %v", err)
		}
		switch readResult.Content.(type) {
		case *api.ReadResp_Event:
			{
				event := readResult.GetEvent()
				recordedEvent := protoutils.RecordedEventFromProto(event)
				events = append(events, recordedEvent)
				if uint64(len(events)) >= limit {
					break
				}
			}
		case *api.ReadResp_StreamNotFound_:
			{
				return nil, errors.ErrStreamNotFound
			}
		}
	}
	return events, nil
}
