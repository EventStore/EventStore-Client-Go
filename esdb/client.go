package esdb

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"errors"

	persistentProto "github.com/EventStore/EventStore-Client-Go/protos/persistent"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	api "github.com/EventStore/EventStore-Client-Go/protos/streams"
)

// Client ...
type Client struct {
	grpcClient *grpcClient
	Config     *Configuration
}

// NewClient ...
func NewClient(configuration *Configuration) (*Client, error) {
	grpcClient := NewGrpcClient(*configuration)
	return &Client{
		grpcClient: grpcClient,
		Config:     configuration,
	}, nil
}

// Close ...
func (client *Client) Close() error {
	client.grpcClient.close()
	return nil
}

// AppendToStream ...
func (client *Client) AppendToStream(
	context context.Context,
	streamID string,
	opts AppendToStreamOptions,
	events ...EventData,
) (*WriteResult, error) {
	opts.setDefaults()
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return nil, fmt.Errorf("can't get a connection handle: %w", err)
	}
	streamsClient := api.NewStreamsClient(handle.Connection())
	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	if opts.Authenticated != nil {
		callOptions = append(callOptions, grpc.PerRPCCredentials(basicAuth{
			username: opts.Authenticated.Login,
			password: opts.Authenticated.Password,
		}))
	}

	appendOperation, err := streamsClient.Append(context, callOptions...)
	if err != nil {
		err = client.grpcClient.handleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("could not construct append operation. Reason: %w", err)
	}

	header := toAppendHeader(streamID, opts.ExpectedRevision)

	if err := appendOperation.Send(header); err != nil {
		err = client.grpcClient.handleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("could not send append request header. Reason: %w", err)
	}

	for _, event := range events {
		appendRequest := &api.AppendReq{
			Content: &api.AppendReq_ProposedMessage_{
				ProposedMessage: toProposedMessage(event),
			},
		}

		if err = appendOperation.Send(appendRequest); err != nil {
			err = client.grpcClient.handleError(handle, headers, trailers, err)
			return nil, fmt.Errorf("could not send append request. Reason: %w", err)
		}
	}

	response, err := appendOperation.CloseAndRecv()
	if err != nil {
		return nil, client.grpcClient.handleError(handle, headers, trailers, err)
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
			return nil, ErrWrongExpectedStreamRevision
		}
	}

	return &WriteResult{
		CommitPosition:      0,
		PreparePosition:     0,
		NextExpectedVersion: 1,
	}, nil
}

func (client *Client) SetStreamMetadata(
	context context.Context,
	streamID string,
	opts AppendToStreamOptions,
	metadata StreamMetadata,
) (*WriteResult, error) {
	streamName := fmt.Sprintf("$$%v", streamID)
	props, err := metadata.ToMap()

	if err != nil {
		return nil, fmt.Errorf("error when serializing stream metadata: %w", err)
	}

	data, err := json.Marshal(props)

	if err != nil {
		return nil, fmt.Errorf("error when serializing stream metadata: %w", err)
	}

	result, err := client.AppendToStream(context, streamName, opts, EventData{
		ContentType: JsonContentType,
		EventType:   "$metadata",
		Data:        data,
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

func (client *Client) GetStreamMetadata(
	context context.Context,
	streamID string,
	opts ReadStreamOptions,
) (*StreamMetadata, error) {
	streamName := fmt.Sprintf("$$%v", streamID)

	stream, err := client.ReadStream(context, streamName, opts, 1)

	var streamDeletedError *StreamDeletedError
	if errors.Is(err, ErrStreamNotFound) || errors.As(err, &streamDeletedError) {
		return nil, err
	}

	if err != nil {
		return nil, fmt.Errorf("unexpected error when reading stream metadata: %w", err)
	}

	event, err := stream.Recv()

	if errors.Is(err, io.EOF) {
		return &StreamMetadata{}, nil
	}

	if err != nil {
		return nil, fmt.Errorf("unexpected error when reading stream metadata: %w", err)
	}

	var props map[string]interface{}

	err = json.Unmarshal(event.OriginalEvent().Data, &props)

	if err != nil {
		return nil, fmt.Errorf("error when deserializing stream metadata json: %w", err)
	}

	meta, err := StreamMetadataFromMap(props)

	if err != nil {
		return nil, fmt.Errorf("error when parsing stream metadata json: %w", err)
	}

	return &meta, nil
}

// DeleteStream ...
func (client *Client) DeleteStream(
	context context.Context,
	streamID string,
	opts DeleteStreamOptions,
) (*DeleteResult, error) {
	opts.setDefaults()
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return nil, fmt.Errorf("can't get a connection handle: %w", err)
	}
	streamsClient := api.NewStreamsClient(handle.Connection())
	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	if opts.Authenticated != nil {
		callOptions = append(callOptions, grpc.PerRPCCredentials(basicAuth{
			username: opts.Authenticated.Login,
			password: opts.Authenticated.Password,
		}))
	}
	deleteRequest := toDeleteRequest(streamID, opts.ExpectedRevision)
	deleteResponse, err := streamsClient.Delete(context, deleteRequest, callOptions...)
	if err != nil {
		err = client.grpcClient.handleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("failed to perform delete, details: %w", err)
	}

	return &DeleteResult{Position: deletePositionFromProto(deleteResponse)}, nil
}

// Tombstone ...
func (client *Client) TombstoneStream(
	context context.Context,
	streamID string,
	opts TombstoneStreamOptions,
) (*DeleteResult, error) {
	opts.setDefaults()
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return nil, fmt.Errorf("can't get a connection handle: %w", err)
	}
	streamsClient := api.NewStreamsClient(handle.Connection())
	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	if opts.Authenticated != nil {
		callOptions = append(callOptions, grpc.PerRPCCredentials(basicAuth{
			username: opts.Authenticated.Login,
			password: opts.Authenticated.Password,
		}))
	}
	tombstoneRequest := toTombstoneRequest(streamID, opts.ExpectedRevision)
	tombstoneResponse, err := streamsClient.Tombstone(context, tombstoneRequest, callOptions...)

	if err != nil {
		err = client.grpcClient.handleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("failed to perform delete, details: %w", err)
	}

	return &DeleteResult{Position: tombstonePositionFromProto(tombstoneResponse)}, nil
}

// ReadStream ...
func (client *Client) ReadStream(
	context context.Context,
	streamID string,
	opts ReadStreamOptions,
	count uint64,
) (*ReadStream, error) {
	opts.setDefaults()
	readRequest := toReadStreamRequest(streamID, opts.Direction, opts.From, count, opts.ResolveLinkTos)
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return nil, fmt.Errorf("can't get a connection handle: %w", err)
	}
	streamsClient := api.NewStreamsClient(handle.Connection())

	return readInternal(context, client.grpcClient, handle, streamsClient, readRequest, opts.Authenticated)
}

// ReadAll ...
func (client *Client) ReadAll(
	context context.Context,
	opts ReadAllOptions,
	count uint64,
) (*ReadStream, error) {
	opts.setDefaults()
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return nil, fmt.Errorf("can't get a connection handle: %w", err)
	}
	streamsClient := api.NewStreamsClient(handle.Connection())
	readRequest := toReadAllRequest(opts.Direction, opts.From, count, opts.ResolveLinkTos)
	return readInternal(context, client.grpcClient, handle, streamsClient, readRequest, opts.Authenticated)
}

// SubscribeToStream ...
func (client *Client) SubscribeToStream(
	ctx context.Context,
	streamID string,
	opts SubscribeToStreamOptions,
) (*Subscription, error) {
	opts.setDefaults()
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return nil, fmt.Errorf("can't get a connection handle: %w", err)
	}
	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	if opts.Authenticated != nil {
		callOptions = append(callOptions, grpc.PerRPCCredentials(basicAuth{
			username: opts.Authenticated.Login,
			password: opts.Authenticated.Password,
		}))
	}
	streamsClient := api.NewStreamsClient(handle.Connection())
	subscriptionRequest, err := toStreamSubscriptionRequest(streamID, opts.From, opts.ResolveLinkTos, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to construct subscription. Reason: %w", err)
	}
	ctx, cancel := context.WithCancel(ctx)
	readClient, err := streamsClient.Read(ctx, subscriptionRequest, callOptions...)
	if err != nil {
		defer cancel()
		err = client.grpcClient.handleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("failed to construct subscription. Reason: %w", err)
	}
	readResult, err := readClient.Recv()
	if err != nil {
		defer cancel()
		err = client.grpcClient.handleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("failed to perform read. Reason: %w", err)
	}
	switch readResult.Content.(type) {
	case *api.ReadResp_Confirmation:
		{
			confirmation := readResult.GetConfirmation()
			return NewSubscription(client, cancel, readClient, confirmation.SubscriptionId), nil
		}
	}
	defer cancel()
	return nil, fmt.Errorf("failed to initiate subscription")
}

// SubscribeToAll ...
func (client *Client) SubscribeToAll(
	ctx context.Context,
	opts SubscribeToAllOptions,
) (*Subscription, error) {
	opts.setDefaults()
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return nil, fmt.Errorf("can't get a connection handle: %w", err)
	}
	streamsClient := api.NewStreamsClient(handle.Connection())
	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	if opts.Authenticated != nil {
		callOptions = append(callOptions, grpc.PerRPCCredentials(basicAuth{
			username: opts.Authenticated.Login,
			password: opts.Authenticated.Password,
		}))
	}

	var filterOptions *SubscriptionFilterOptions = nil
	if opts.Filter != nil {
		filterOptions = &SubscriptionFilterOptions{
			MaxSearchWindow:    opts.MaxSearchWindow,
			CheckpointInterval: opts.CheckpointInterval,
			SubscriptionFilter: opts.Filter,
		}
	}

	subscriptionRequest, err := toAllSubscriptionRequest(opts.From, opts.ResolveLinkTos, filterOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to construct subscription. Reason: %w", err)
	}
	ctx, cancel := context.WithCancel(ctx)
	readClient, err := streamsClient.Read(ctx, subscriptionRequest, callOptions...)
	if err != nil {
		defer cancel()
		err = client.grpcClient.handleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("failed to construct subscription. Reason: %w", err)
	}
	readResult, err := readClient.Recv()
	if err != nil {
		defer cancel()
		err = client.grpcClient.handleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("failed to perform read. Reason: %w", err)
	}
	switch readResult.Content.(type) {
	case *api.ReadResp_Confirmation:
		{
			confirmation := readResult.GetConfirmation()
			return NewSubscription(client, cancel, readClient, confirmation.SubscriptionId), nil
		}
	}
	defer cancel()
	return nil, fmt.Errorf("failed to initiate subscription")
}

// ConnectToPersistentSubscription ...
func (client *Client) ConnectToPersistentSubscription(
	ctx context.Context,
	streamName string,
	groupName string,
	options ConnectToPersistentSubscriptionOptions,
) (*PersistentSubscription, error) {
	options.setDefaults()
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return nil, fmt.Errorf("can't get a connection handle: %w", err)
	}
	persistentSubscriptionClient := newPersistentClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

	return persistentSubscriptionClient.ConnectToPersistentSubscription(
		ctx,
		handle,
		int32(options.BatchSize),
		streamName,
		groupName,
		options.Authenticated,
	)
}

func (client *Client) CreatePersistentSubscription(
	ctx context.Context,
	streamName string,
	groupName string,
	options PersistentStreamSubscriptionOptions,
) error {
	options.setDefaults()
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return fmt.Errorf("can't get a connection handle: %w", err)
	}
	persistentSubscriptionClient := newPersistentClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

	if options.Settings == nil {
		setts := SubscriptionSettingsDefault()
		options.Settings = &setts
	}

	return persistentSubscriptionClient.CreateStreamSubscription(ctx, handle, streamName, groupName, options.From, *options.Settings, options.Authenticated)
}

func (client *Client) CreatePersistentSubscriptionAll(
	ctx context.Context,
	groupName string,
	options PersistentAllSubscriptionOptions,
) error {
	options.setDefaults()
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return fmt.Errorf("can't get a connection handle: %w", err)
	}

	var filterOptions *SubscriptionFilterOptions = nil
	if options.Filter != nil {
		filterOptions = &SubscriptionFilterOptions{
			MaxSearchWindow:    options.MaxSearchWindow,
			CheckpointInterval: options.CheckpointInterval,
			SubscriptionFilter: options.Filter,
		}
	}
	persistentSubscriptionClient := newPersistentClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

	if options.Settings == nil {
		setts := SubscriptionSettingsDefault()
		options.Settings = &setts
	}

	return persistentSubscriptionClient.CreateAllSubscription(
		ctx,
		handle,
		groupName,
		options.From,
		*options.Settings,
		filterOptions,
		options.Authenticated,
	)
}

func (client *Client) UpdatePersistentStreamSubscription(
	ctx context.Context,
	streamName string,
	groupName string,
	options PersistentStreamSubscriptionOptions,
) error {
	options.setDefaults()
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return fmt.Errorf("can't get a connection handle: %w", err)
	}
	persistentSubscriptionClient := newPersistentClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

	if options.Settings == nil {
		setts := SubscriptionSettingsDefault()
		options.Settings = &setts
	}

	return persistentSubscriptionClient.UpdateStreamSubscription(ctx, handle, streamName, groupName, options.From, *options.Settings, options.Authenticated)
}

func (client *Client) UpdatePersistentSubscriptionAll(
	ctx context.Context,
	groupName string,
	options PersistentAllSubscriptionOptions,
) error {
	options.setDefaults()
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return fmt.Errorf("can't get a connection handle: %w", err)
	}
	persistentSubscriptionClient := newPersistentClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

	return persistentSubscriptionClient.UpdateAllSubscription(ctx, handle, groupName, options.From, *options.Settings, options.Authenticated)
}

func (client *Client) DeletePersistentSubscription(
	ctx context.Context,
	streamName string,
	groupName string,
	options DeletePersistentSubscriptionOptions,
) error {
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return fmt.Errorf("can't get a connection handle: %w", err)
	}
	persistentSubscriptionClient := newPersistentClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

	return persistentSubscriptionClient.DeleteStreamSubscription(ctx, handle, streamName, groupName, options.Authenticated)
}

func (client *Client) DeletePersistentSubscriptionAll(
	ctx context.Context,
	groupName string,
	options DeletePersistentSubscriptionOptions,
) error {
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return fmt.Errorf("can't get a connection handle: %w", err)
	}
	persistentSubscriptionClient := newPersistentClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

	return persistentSubscriptionClient.DeleteAllSubscription(ctx, handle, groupName, options.Authenticated)
}

func readInternal(
	ctx context.Context,
	client *grpcClient,
	handle connectionHandle,
	streamsClient api.StreamsClient,
	readRequest *api.ReadReq,
	auth *Credentials,
) (*ReadStream, error) {
	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	if auth != nil {
		callOptions = append(callOptions, grpc.PerRPCCredentials(basicAuth{
			username: auth.Login,
			password: auth.Password,
		}))
	}
	ctx, cancel := context.WithCancel(ctx)
	result, err := streamsClient.Read(ctx, readRequest, callOptions...)
	if err != nil {
		defer cancel()

		err = client.handleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("failed to construct read stream. Reason: %w", err)
	}

	msg, err := result.Recv()
	if err != nil {
		defer cancel()
		values := trailers.Get("exception")

		if values != nil && values[0] == "stream-deleted" {
			values = trailers.Get("stream-name")
			streamName := ""

			if values != nil {
				streamName = values[0]
			}

			return nil, &StreamDeletedError{StreamName: streamName}
		}

		return nil, err
	}

	switch msg.Content.(type) {
	case *api.ReadResp_Event:
		resolvedEvent := getResolvedEventFromProto(msg.GetEvent())
		params := readStreamParams{
			client:   client,
			handle:   handle,
			cancel:   cancel,
			inner:    result,
			headers:  headers,
			trailers: trailers,
		}

		stream := newReadStream(params, resolvedEvent)
		return stream, nil
	case *api.ReadResp_StreamNotFound_:
		defer cancel()
		return nil, ErrStreamNotFound
	}

	defer cancel()
	return nil, fmt.Errorf("unexpected code path in readInternal")
}
