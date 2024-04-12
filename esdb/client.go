// Package esdb EventStoreDB gRPC client.
package esdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"

	persistentProto "github.com/EventStore/EventStore-Client-Go/v4/protos/persistent"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	api "github.com/EventStore/EventStore-Client-Go/v4/protos/streams"
)

// Client Represents a client to a single node. A client instance maintains a full duplex communication to EventStoreDB.
// Many threads can use an EventStoreDB client at the same time or a single thread can make many asynchronous requests.
type Client struct {
	grpcClient *grpcClient
	config     *Configuration
}

// NewClient Creates a gRPC client to an EventStoreDB database.
func NewClient(configuration *Configuration) (*Client, error) {
	grpcClient := newGrpcClient(*configuration)
	return &Client{
		grpcClient: grpcClient,
		config:     configuration,
	}, nil
}

// Close Closes a connection and cleans all its allocated resources.
func (client *Client) Close() error {
	client.grpcClient.close()
	return nil
}

// AppendToStream Appends events to a given stream.
func (client *Client) AppendToStream(
	context context.Context,
	streamID string,
	opts AppendToStreamOptions,
	events ...EventData,
) (*WriteResult, error) {
	opts.setDefaults()
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return nil, err
	}
	streamsClient := api.NewStreamsClient(handle.Connection())
	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	callOptions, ctx, cancel := configureGrpcCall(context, client.config, &opts, callOptions, client.grpcClient.perRPCCredentials)
	defer cancel()

	appendOperation, err := streamsClient.Append(ctx, callOptions...)
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
			wrong := result.(*api.AppendResp_WrongExpectedVersion_).WrongExpectedVersion
			expected := ""
			current := ""

			if wrong.GetExpectedAny() != nil {
				expected = "any"
			} else if wrong.GetExpectedNoStream() != nil {
				expected = "no_stream"
			} else if wrong.GetExpectedStreamExists() != nil {
				expected = "stream_exists"
			} else {
				expected = strconv.Itoa(int(wrong.GetExpectedRevision()))
			}

			if wrong.GetCurrentNoStream() != nil {
				current = "no_stream"
			} else {
				current = strconv.Itoa(int(wrong.GetCurrentRevision()))
			}

			return nil, &Error{code: ErrorCodeWrongExpectedVersion, err: fmt.Errorf("wrong expected version: expecting '%s' but got '%s'", expected, current)}
		}
	}

	return &WriteResult{
		CommitPosition:      0,
		PreparePosition:     0,
		NextExpectedVersion: 1,
	}, nil
}

// SetStreamMetadata Sets the metadata for a stream.
func (client *Client) SetStreamMetadata(
	context context.Context,
	streamID string,
	opts AppendToStreamOptions,
	metadata StreamMetadata,
) (*WriteResult, error) {
	streamName := fmt.Sprintf("$$%v", streamID)
	props, err := metadata.toMap()

	if err != nil {
		return nil, fmt.Errorf("error when serializing stream metadata: %w", err)
	}

	data, err := json.Marshal(props)

	if err != nil {
		return nil, fmt.Errorf("error when serializing stream metadata: %w", err)
	}

	result, err := client.AppendToStream(context, streamName, opts, EventData{
		ContentType: ContentTypeJson,
		EventType:   "$metadata",
		Data:        data,
	})

	if err != nil {
		return nil, err
	}

	return result, nil
}

// GetStreamMetadata Reads the metadata for a stream.
func (client *Client) GetStreamMetadata(
	context context.Context,
	streamID string,
	opts ReadStreamOptions,
) (*StreamMetadata, error) {
	streamName := fmt.Sprintf("$$%v", streamID)

	stream, err := client.ReadStream(context, streamName, opts, 1)

	if esdbErr, ok := FromError(err); !ok {
		return nil, esdbErr
	}

	defer stream.Close()
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
		return nil, &Error{code: ErrorCodeParsing, err: fmt.Errorf("error when deserializing stream metadata json: %w", err)}
	}

	meta, err := streamMetadataFromMap(props)

	if err != nil {
		return nil, &Error{code: ErrorCodeParsing, err: fmt.Errorf("error when parsing stream metadata json: %w", err)}
	}

	return meta, nil
}

// DeleteStream Deletes a given stream.
//
// Makes use of "Truncate Before". When a stream is deleted, it's "Truncate Before" is set to the stream's current last
// event number. When a deleted stream is read, the read will return a stream not found error. After deleting
// the stream, you are able to write to it again, continuing from where it left off.
func (client *Client) DeleteStream(
	parent context.Context,
	streamID string,
	opts DeleteStreamOptions,
) (*DeleteResult, error) {
	opts.setDefaults()
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return nil, err
	}
	streamsClient := api.NewStreamsClient(handle.Connection())
	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	callOptions, ctx, cancel := configureGrpcCall(parent, client.config, &opts, callOptions, client.grpcClient.perRPCCredentials)
	defer cancel()
	deleteRequest := toDeleteRequest(streamID, opts.ExpectedRevision)
	deleteResponse, err := streamsClient.Delete(ctx, deleteRequest, callOptions...)
	if err != nil {
		err = client.grpcClient.handleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("failed to perform delete, details: %w", err)
	}

	return &DeleteResult{Position: deletePositionFromProto(deleteResponse)}, nil
}

// TombstoneStream Permanently deletes a given stream by writing a Tombstone event to the end of the stream.
//
// A Tombstone event is written to the end of the stream, permanently deleting it. The stream cannot be recreated or
// written to again. Tombstone events are written with the event's type "$streamDeleted". When a tombstoned stream
// is read, the read will return a stream deleted error.
func (client *Client) TombstoneStream(
	parent context.Context,
	streamID string,
	opts TombstoneStreamOptions,
) (*DeleteResult, error) {
	opts.setDefaults()
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return nil, err
	}
	streamsClient := api.NewStreamsClient(handle.Connection())
	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	callOptions, ctx, cancel := configureGrpcCall(parent, client.config, &opts, callOptions, client.grpcClient.perRPCCredentials)
	defer cancel()
	tombstoneRequest := toTombstoneRequest(streamID, opts.ExpectedRevision)
	tombstoneResponse, err := streamsClient.Tombstone(ctx, tombstoneRequest, callOptions...)

	if err != nil {
		err = client.grpcClient.handleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("failed to perform delete, details: %w", err)
	}

	return &DeleteResult{Position: tombstonePositionFromProto(tombstoneResponse)}, nil
}

// ReadStream Reads events from a given stream. The reading can be done forward and backward.
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
		return nil, err
	}
	streamsClient := api.NewStreamsClient(handle.Connection())

	return readInternal(context, client, &opts, handle, streamsClient, readRequest)
}

// ReadAll Reads events from the $all stream. The reading can be done forward and backward.
func (client *Client) ReadAll(
	context context.Context,
	opts ReadAllOptions,
	count uint64,
) (*ReadStream, error) {
	opts.setDefaults()
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return nil, err
	}
	streamsClient := api.NewStreamsClient(handle.Connection())
	readRequest := toReadAllRequest(opts.Direction, opts.From, count, opts.ResolveLinkTos)
	return readInternal(context, client, &opts, handle, streamsClient, readRequest)
}

// SubscribeToStream allows you to subscribe to a stream and receive notifications about new events added to the stream.
// The subscription will notify event from the starting point onward. If events already exist, the handler will be
// called for each event one by one until it reaches the end of the stream. From there, the server will notify the
// handler whenever a new event appears.
func (client *Client) SubscribeToStream(
	parent context.Context,
	streamID string,
	opts SubscribeToStreamOptions,
) (*Subscription, error) {
	opts.setDefaults()
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return nil, err
	}
	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	callOptions, ctx, cancel := configureGrpcCall(parent, client.config, &opts, callOptions, client.grpcClient.perRPCCredentials)

	streamsClient := api.NewStreamsClient(handle.Connection())
	subscriptionRequest, err := toStreamSubscriptionRequest(streamID, opts.From, opts.ResolveLinkTos, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to construct subscription. Reason: %w", err)
	}
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
			return newSubscription(client, cancel, readClient, confirmation.SubscriptionId), nil
		}
	}
	defer cancel()
	return nil, fmt.Errorf("failed to initiate subscription")
}

// SubscribeToAll allows you to subscribe to $all stream and receive notifications about new events added to the stream.
// The subscription will notify event from the starting point onward. If events already exist, the handler will be
// called for each event one by one until it reaches the end of the stream. From there, the server will notify the
// handler whenever a new event appears.
func (client *Client) SubscribeToAll(
	parent context.Context,
	opts SubscribeToAllOptions,
) (*Subscription, error) {
	opts.setDefaults()
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return nil, err
	}
	streamsClient := api.NewStreamsClient(handle.Connection())
	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	callOptions, ctx, cancel := configureGrpcCall(parent, client.config, &opts, callOptions, client.grpcClient.perRPCCredentials)

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
			return newSubscription(client, cancel, readClient, confirmation.SubscriptionId), nil
		}
	}
	defer cancel()
	return nil, fmt.Errorf("failed to initiate subscription")
}

// SubscribeToPersistentSubscription Connects to a persistent subscription group on a stream.
func (client *Client) SubscribeToPersistentSubscription(
	ctx context.Context,
	streamName string,
	groupName string,
	options SubscribeToPersistentSubscriptionOptions,
) (*PersistentSubscription, error) {
	options.setDefaults()
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return nil, err
	}

	persistentSubscriptionClient := newPersistentClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

	return persistentSubscriptionClient.ConnectToPersistentSubscription(
		ctx,
		client.config,
		&options,
		handle,
		int32(options.BufferSize),
		streamName,
		groupName,
	)
}

// SubscribeToPersistentSubscriptionToAll Connects to a persistent subscription group to the $all stream.
func (client *Client) SubscribeToPersistentSubscriptionToAll(
	ctx context.Context,
	groupName string,
	options SubscribeToPersistentSubscriptionOptions,
) (*PersistentSubscription, error) {
	options.setDefaults()
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return nil, err
	}

	if !handle.SupportsFeature(featurePersistentSubscriptionToAll) {
		return nil, unsupportedFeatureError()
	}
	persistentSubscriptionClient := newPersistentClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

	return persistentSubscriptionClient.ConnectToPersistentSubscription(
		ctx,
		client.config,
		&options,
		handle,
		int32(options.BufferSize),
		"",
		groupName,
	)
}

// CreatePersistentSubscription Creates a persistent subscription gorup on a stream.
//
// Persistent subscriptions are special kind of subscription where the server remembers the state of the subscription.
// This allows for many modes of operations compared to a regular or catcup subscription where the client
// holds the subscription state.
func (client *Client) CreatePersistentSubscription(
	ctx context.Context,
	streamName string,
	groupName string,
	options PersistentStreamSubscriptionOptions,
) error {
	options.setDefaults()
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return err
	}
	persistentSubscriptionClient := newPersistentClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

	if options.Settings == nil {
		setts := SubscriptionSettingsDefault()
		options.Settings = &setts
	}

	return persistentSubscriptionClient.CreateStreamSubscription(ctx, client.config, &options, handle, streamName, groupName, options.StartFrom, *options.Settings)
}

// CreatePersistentSubscriptionToAll Creates a persistent subscription gorup on the $all stream.
//
// Persistent subscriptions are special kind of subscription where the server remembers the state of the subscription.
// This allows for many modes of operations compared to a regular or catcup subscription where the client
// holds the subscription state.
func (client *Client) CreatePersistentSubscriptionToAll(
	ctx context.Context,
	groupName string,
	options PersistentAllSubscriptionOptions,
) error {
	options.setDefaults()
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return err
	}

	if !handle.SupportsFeature(featurePersistentSubscriptionToAll) {
		return unsupportedFeatureError()
	}
	var filterOptions *SubscriptionFilterOptions = nil
	if options.Filter != nil {
		filterOptions = &SubscriptionFilterOptions{
			MaxSearchWindow:    options.MaxSearchWindow,
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
		client.config,
		&options,
		handle,
		groupName,
		options.StartFrom,
		*options.Settings,
		filterOptions,
	)
}

// UpdatePersistentSubscription Updates a persistent subscription group on a stream.
func (client *Client) UpdatePersistentSubscription(
	ctx context.Context,
	streamName string,
	groupName string,
	options PersistentStreamSubscriptionOptions,
) error {
	options.setDefaults()
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return err
	}
	persistentSubscriptionClient := newPersistentClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

	if options.Settings == nil {
		setts := SubscriptionSettingsDefault()
		options.Settings = &setts
	}

	return persistentSubscriptionClient.UpdateStreamSubscription(ctx, client.config, &options, handle, streamName, groupName, options.StartFrom, *options.Settings)
}

// UpdatePersistentSubscriptionToAll Updates a persistent subscription group on the $all stream.
func (client *Client) UpdatePersistentSubscriptionToAll(
	ctx context.Context,
	groupName string,
	options PersistentAllSubscriptionOptions,
) error {
	options.setDefaults()
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return err
	}

	if !handle.SupportsFeature(featurePersistentSubscriptionToAll) {
		return unsupportedFeatureError()
	}

	persistentSubscriptionClient := newPersistentClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

	return persistentSubscriptionClient.UpdateAllSubscription(ctx, client.config, &options, handle, groupName, options.StartFrom, *options.Settings)
}

// DeletePersistentSubscription Deletes a persistent subscription group on a stream.
func (client *Client) DeletePersistentSubscription(
	ctx context.Context,
	streamName string,
	groupName string,
	options DeletePersistentSubscriptionOptions,
) error {
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return err
	}
	persistentSubscriptionClient := newPersistentClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

	return persistentSubscriptionClient.DeleteStreamSubscription(ctx, client.config, &options, handle, streamName, groupName)
}

// DeletePersistentSubscriptionToAll Deletes a persistent subscription group on the $all stream.
func (client *Client) DeletePersistentSubscriptionToAll(
	ctx context.Context,
	groupName string,
	options DeletePersistentSubscriptionOptions,
) error {
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return err
	}

	if !handle.SupportsFeature(featurePersistentSubscriptionToAll) {
		return unsupportedFeatureError()
	}

	persistentSubscriptionClient := newPersistentClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))

	return persistentSubscriptionClient.DeleteAllSubscription(ctx, client.config, &options, handle, groupName)
}

// ReplayParkedMessages Replays the parked messages of a persistent subscription to a stream.
func (client *Client) ReplayParkedMessages(ctx context.Context, streamName string, groupName string, options ReplayParkedMessagesOptions) error {
	return client.replayParkedMessages(ctx, streamName, groupName, options)
}

// ReplayParkedMessagesToAll Replays the parked messages of a persistent subscription to $all.
func (client *Client) ReplayParkedMessagesToAll(ctx context.Context, groupName string, options ReplayParkedMessagesOptions) error {
	return client.replayParkedMessages(ctx, "$all", groupName, options)
}

func (client *Client) replayParkedMessages(ctx context.Context, streamName string, groupName string, options ReplayParkedMessagesOptions) error {
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return err
	}

	var finalStreamName *string
	if streamName != "$all" {
		finalStreamName = &streamName
	}

	if finalStreamName == nil && !handle.SupportsFeature(featurePersistentSubscriptionToAll) {
		return unsupportedFeatureError()
	}

	if handle.SupportsFeature(featurePersistentSubscriptionManagement) {
		persistentSubscriptionClient := newPersistentClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))
		return persistentSubscriptionClient.replayParkedMessages(ctx, client.config, handle, finalStreamName, groupName, &options)
	}

	return client.httpReplayParkedMessages(streamName, groupName, options)
}

// ListAllPersistentSubscriptions Lists all persistent subscriptions regardless of which stream they are on.
func (client *Client) ListAllPersistentSubscriptions(ctx context.Context, options ListPersistentSubscriptionsOptions) ([]PersistentSubscriptionInfo, error) {
	return client.listPersistentSubscriptionsInternal(ctx, nil, options)
}

// ListPersistentSubscriptionsForStream Lists all persistent subscriptions of a specific stream.
func (client *Client) ListPersistentSubscriptionsForStream(ctx context.Context, streamName string, options ListPersistentSubscriptionsOptions) ([]PersistentSubscriptionInfo, error) {
	return client.listPersistentSubscriptionsInternal(ctx, &streamName, options)
}

// ListPersistentSubscriptionsToAll Lists all persistent subscriptions specific to the $all stream.
func (client *Client) ListPersistentSubscriptionsToAll(ctx context.Context, options ListPersistentSubscriptionsOptions) ([]PersistentSubscriptionInfo, error) {
	streamName := "$all"
	return client.listPersistentSubscriptionsInternal(ctx, &streamName, options)
}

func (client *Client) listPersistentSubscriptionsInternal(ctx context.Context, streamName *string, options ListPersistentSubscriptionsOptions) ([]PersistentSubscriptionInfo, error) {
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return nil, err
	}

	if streamName != nil && *streamName == "$all" && !handle.SupportsFeature(featurePersistentSubscriptionToAll) {
		return nil, unsupportedFeatureError()
	}

	if handle.SupportsFeature(featurePersistentSubscriptionManagement) {
		persistentSubscriptionClient := newPersistentClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))
		return persistentSubscriptionClient.listPersistentSubscriptions(ctx, client.config, handle, streamName, &options)
	}

	if streamName != nil {
		return client.httpListPersistentSubscriptionsForStream(*streamName, options)
	}

	return client.httpListAllPersistentSubscriptions(options)
}

// GetPersistentSubscriptionInfo Gets the info for a specific persistent subscription to a stream
func (client *Client) GetPersistentSubscriptionInfo(ctx context.Context, streamName string, groupName string, options GetPersistentSubscriptionOptions) (*PersistentSubscriptionInfo, error) {
	return client.getPersistentSubscriptionInfoInternal(ctx, &streamName, groupName, options)
}

// GetPersistentSubscriptionInfoToAll Gets the info for a specific persistent subscription to the $all stream.
func (client *Client) GetPersistentSubscriptionInfoToAll(ctx context.Context, groupName string, options GetPersistentSubscriptionOptions) (*PersistentSubscriptionInfo, error) {
	return client.getPersistentSubscriptionInfoInternal(ctx, nil, groupName, options)
}

func (client *Client) getPersistentSubscriptionInfoInternal(ctx context.Context, streamName *string, groupName string, options GetPersistentSubscriptionOptions) (*PersistentSubscriptionInfo, error) {
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return nil, err
	}

	if streamName != nil && *streamName == "$all" && !handle.SupportsFeature(featurePersistentSubscriptionToAll) {
		return nil, unsupportedFeatureError()

	}

	if handle.SupportsFeature(featurePersistentSubscriptionManagement) {
		persistentSubscriptionClient := newPersistentClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))
		return persistentSubscriptionClient.getPersistentSubscriptionInfo(ctx, client.config, handle, streamName, groupName, &options)
	}

	if streamName == nil {
		streamName = new(string)
		*streamName = "$all"
	}

	return client.httpGetPersistentSubscriptionInfo(*streamName, groupName, options)
}

// RestartPersistentSubscriptionSubsystem Restarts the persistent subscription subsystem on the server.
func (client *Client) RestartPersistentSubscriptionSubsystem(ctx context.Context, options RestartPersistentSubscriptionSubsystemOptions) error {
	handle, err := client.grpcClient.getConnectionHandle()
	if err != nil {
		return err
	}

	if handle.SupportsFeature(featurePersistentSubscriptionManagement) {
		persistentClient := newPersistentClient(client.grpcClient, persistentProto.NewPersistentSubscriptionsClient(handle.Connection()))
		return persistentClient.restartSubsystem(ctx, client.config, handle, &options)
	}

	return client.httpRestartSubsystem(options)
}

func readInternal(
	parent context.Context,
	client *Client,
	options options,
	handle *connectionHandle,
	streamsClient api.StreamsClient,
	readRequest *api.ReadReq,
) (*ReadStream, error) {
	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	callOptions, ctx, cancel := configureGrpcCall(parent, client.config, options, callOptions, client.grpcClient.perRPCCredentials)
	result, err := streamsClient.Read(ctx, readRequest, callOptions...)
	if err != nil {
		defer cancel()
		return nil, err
	}

	params := readStreamParams{
		client:   client.grpcClient,
		handle:   handle,
		cancel:   cancel,
		inner:    result,
		headers:  &headers,
		trailers: &trailers,
	}

	return newReadStream(params), nil
}
