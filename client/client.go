package client

import (
	"context"
	"fmt"

	"github.com/EventStore/EventStore-Client-Go/stream_position"

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
	api "github.com/EventStore/EventStore-Client-Go/protos/streams"
	stream_revision "github.com/EventStore/EventStore-Client-Go/streamrevision"
)

type Configuration = connection.Configuration

func ParseConnectionString(str string) (*connection.Configuration, error) {
	return connection.ParseConnectionString(str)
}

// Client ...
type Client struct {
	grpcClient              connection.GrpcClient
	Config                  *connection.Configuration
	persistentClientFactory persistent.ClientFactory
}

// NewClient ...
func NewClient(configuration *connection.Configuration) (*Client, error) {
	grpcClient := connection.NewGrpcClient(*configuration)
	return &Client{
		grpcClient:              grpcClient,
		Config:                  configuration,
		persistentClientFactory: persistent.ClientFactoryImpl{},
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

// ReadAllEvents ...
func (client *Client) ReadAllEvents(
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

// SubscribeToStream ...
func (client *Client) SubscribeToStream(
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

// SubscribeToAll ...
func (client *Client) SubscribeToAll(
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

// SubscribeToAllFiltered ...
func (client *Client) SubscribeToAllFiltered(
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
	persistentSubscriptionClient := client.persistentClientFactory.
		CreateClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

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
	persistentSubscriptionClient := client.persistentClientFactory.
		CreateClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

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
	persistentSubscriptionClient := client.persistentClientFactory.
		CreateClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

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
	persistentSubscriptionClient := client.persistentClientFactory.
		CreateClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

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
	persistentSubscriptionClient := client.persistentClientFactory.
		CreateClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

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
	persistentSubscriptionClient := client.persistentClientFactory.
		CreateClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

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
	persistentSubscriptionClient := client.persistentClientFactory.
		CreateClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

	return persistentSubscriptionClient.DeleteAllSubscription(ctx, handle, groupName)
}

func readInternal(
	ctx context.Context,
	client connection.GrpcClient,
	handle connection.ConnectionHandle,
	streamsClient api.StreamsClient,
	readRequest *api.ReadReq,
) (*ReadStream, error) {
	var headers, trailers metadata.MD
	ctx, cancel := context.WithCancel(ctx)
	result, err := streamsClient.Read(ctx, readRequest, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		defer cancel()
		err = client.HandleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("failed to construct read stream. Reason: %v", err)
	}

	params := ReadStreamParams{
		client:   client,
		handle:   handle,
		cancel:   cancel,
		inner:    result,
		headers:  headers,
		trailers: trailers,
	}

	stream := NewReadStream(params)

	return stream, nil
}
