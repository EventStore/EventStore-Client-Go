package esdb

import (
	"context"
	"fmt"

	"github.com/EventStore/EventStore-Client-Go/v2/protos/persistent"
	"github.com/EventStore/EventStore-Client-Go/v2/protos/shared"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type persistentClient struct {
	inner                        *grpcClient
	persistentSubscriptionClient persistent.PersistentSubscriptionsClient
}

func (client *persistentClient) ConnectToPersistentSubscription(
	parent context.Context,
	conf *Configuration,
	options options,
	handle *connectionHandle,
	bufferSize int32,
	streamName string,
	groupName string,
) (*PersistentSubscription, error) {
	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	callOptions, ctx, cancel := configureGrpcCall(parent, conf, options, callOptions)
	readClient, err := client.persistentSubscriptionClient.Read(ctx, callOptions...)
	if err != nil {
		defer cancel()
		return nil, client.inner.handleError(handle, headers, trailers, err)
	}

	err = readClient.Send(toPersistentReadRequest(bufferSize, groupName, []byte(streamName)))
	if err != nil {
		defer cancel()
		return nil, client.inner.handleError(handle, headers, trailers, err)
	}

	readResult, err := readClient.Recv()
	if err != nil {
		defer cancel()
		return nil, client.inner.handleError(handle, headers, trailers, err)
	}
	switch readResult.Content.(type) {
	case *persistent.ReadResp_SubscriptionConfirmation_:
		{
			asyncConnection := newPersistentSubscription(
				readClient,
				readResult.GetSubscriptionConfirmation().SubscriptionId,
				cancel, client.inner.logger)

			return asyncConnection, nil
		}
	}

	defer cancel()
	return nil, &Error{code: ErrorCodeUnknown, err: fmt.Errorf("persistent subscription confirmation error")}
}

func (client *persistentClient) CreateStreamSubscription(
	parent context.Context,
	conf *Configuration,
	options options,
	handle *connectionHandle,
	streamName string,
	groupName string,
	position StreamPosition,
	settings PersistentSubscriptionSettings,
) error {
	createSubscriptionConfig := createPersistentRequestProto(streamName, groupName, position, settings)
	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	callOptions, ctx, cancel := configureGrpcCall(parent, conf, options, callOptions)
	defer cancel()
	_, err := client.persistentSubscriptionClient.Create(ctx, createSubscriptionConfig, callOptions...)
	if err != nil {
		return client.inner.handleError(handle, headers, trailers, err)
	}

	return nil
}

func (client *persistentClient) CreateAllSubscription(
	parent context.Context,
	conf *Configuration,
	options options,
	handle *connectionHandle,
	groupName string,
	position AllPosition,
	settings PersistentSubscriptionSettings,
	filter *SubscriptionFilterOptions,
) error {
	protoConfig, err := createPersistentRequestAllOptionsProto(groupName, position, settings, filter)
	if err != nil {
		return err
	}

	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	callOptions, ctx, cancel := configureGrpcCall(parent, conf, options, callOptions)
	defer cancel()

	_, err = client.persistentSubscriptionClient.Create(ctx, protoConfig, callOptions...)
	if err != nil {
		return client.inner.handleError(handle, headers, trailers, err)
	}

	return nil
}

func (client *persistentClient) UpdateStreamSubscription(
	parent context.Context,
	conf *Configuration,
	options options,
	handle *connectionHandle,
	streamName string,
	groupName string,
	position StreamPosition,
	settings PersistentSubscriptionSettings,
) error {
	updateSubscriptionConfig := updatePersistentRequestStreamProto(streamName, groupName, position, settings)
	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	callOptions, ctx, cancel := configureGrpcCall(parent, conf, options, callOptions)
	defer cancel()

	_, err := client.persistentSubscriptionClient.Update(ctx, updateSubscriptionConfig, callOptions...)
	if err != nil {
		return client.inner.handleError(handle, headers, trailers, err)
	}

	return nil
}

func (client *persistentClient) UpdateAllSubscription(
	parent context.Context,
	conf *Configuration,
	options options,
	handle *connectionHandle,
	groupName string,
	position AllPosition,
	settings PersistentSubscriptionSettings,
) error {
	updateSubscriptionConfig := updatePersistentRequestAllOptionsProto(groupName, position, settings)

	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	callOptions, ctx, cancel := configureGrpcCall(parent, conf, options, callOptions)
	defer cancel()

	_, err := client.persistentSubscriptionClient.Update(ctx, updateSubscriptionConfig, callOptions...)
	if err != nil {
		return client.inner.handleError(handle, headers, trailers, err)
	}

	return nil
}

func (client *persistentClient) DeleteStreamSubscription(
	parent context.Context,
	conf *Configuration,
	options options,
	handle *connectionHandle,
	streamName string,
	groupName string,
) error {
	deleteSubscriptionOptions := deletePersistentRequestStreamProto(streamName, groupName)
	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	callOptions, ctx, cancel := configureGrpcCall(parent, conf, options, callOptions)
	defer cancel()

	_, err := client.persistentSubscriptionClient.Delete(ctx, deleteSubscriptionOptions, callOptions...)
	if err != nil {
		return client.inner.handleError(handle, headers, trailers, err)
	}

	return nil
}

func (client *persistentClient) DeleteAllSubscription(
	parent context.Context,
	conf *Configuration,
	options options,
	handle *connectionHandle,
	groupName string,
) error {
	deleteSubscriptionOptions := deletePersistentRequestAllOptionsProto(groupName)
	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	callOptions, ctx, cancel := configureGrpcCall(parent, conf, options, callOptions)
	defer cancel()

	_, err := client.persistentSubscriptionClient.Delete(ctx, deleteSubscriptionOptions, callOptions...)
	if err != nil {
		return client.inner.handleError(handle, headers, trailers, err)
	}

	return nil
}

func (client *persistentClient) listPersistentSubscriptions(
	parent context.Context,
	conf *Configuration,
	handle *connectionHandle,
	streamName *string,
	options ListPersistentSubscriptionsOptions,
) ([]PersistentSubscriptionInfo, error) {
	listOptions := &persistent.ListReq_Options{}

	if streamName == nil {
		listOptions.ListOption = &persistent.ListReq_Options_ListAllSubscriptions{
			ListAllSubscriptions: &shared.Empty{},
		}
	} else if *streamName == "$all" {
		listOptions.ListOption = &persistent.ListReq_Options_ListForStream{
			ListForStream: &persistent.ListReq_StreamOption{
				StreamOption: &persistent.ListReq_StreamOption_All{
					All: &shared.Empty{},
				},
			},
		}
	} else {
		listOptions.ListOption = &persistent.ListReq_Options_ListForStream{
			ListForStream: &persistent.ListReq_StreamOption{
				StreamOption: &persistent.ListReq_StreamOption_Stream{
					Stream: &shared.StreamIdentifier{StreamName: []byte(*streamName)},
				},
			},
		}
	}

	listReq := &persistent.ListReq{
		Options: listOptions,
	}

	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	callOptions, ctx, cancel := configureGrpcCall(parent, conf, options, callOptions)
	defer cancel()
	if options.Authenticated != nil {
		callOptions = append(callOptions, grpc.PerRPCCredentials(basicAuth{
			username: options.Authenticated.Login,
			password: options.Authenticated.Password,
		}))
	}

	resp, err := client.persistentSubscriptionClient.List(ctx, listReq, callOptions...)

	if err != nil {
		return nil, client.inner.handleError(handle, headers, trailers, err)
	}

	var infos []PersistentSubscriptionInfo
	for _, wire := range resp.GetSubscriptions() {
		info, err := subscriptionInfoFromWire(wire)

		if err != nil {
			return nil, err
		}

		infos = append(infos, *info)
	}

	return infos, nil
}

func (client *persistentClient) getPersistentSubscriptionInfo(
	parent context.Context,
	conf *Configuration,
	handle *connectionHandle,
	streamName *string,
	groupName string,
	options GetPersistentSubscriptionOptions,
) (*PersistentSubscriptionInfo, error) {
	getInfoOptions := &persistent.GetInfoReq_Options{}

	if streamName == nil {
		getInfoOptions.StreamOption = &persistent.GetInfoReq_Options_All{All: &shared.Empty{}}
	} else {
		getInfoOptions.StreamOption = &persistent.GetInfoReq_Options_StreamIdentifier{
			StreamIdentifier: &shared.StreamIdentifier{StreamName: []byte(*streamName)},
		}
	}

	getInfoOptions.GroupName = groupName

	getInfoReq := &persistent.GetInfoReq{Options: getInfoOptions}

	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	callOptions, ctx, cancel := configureGrpcCall(parent, conf, options, callOptions)
	defer cancel()
	if options.Authenticated != nil {
		callOptions = append(callOptions, grpc.PerRPCCredentials(basicAuth{
			username: options.Authenticated.Login,
			password: options.Authenticated.Password,
		}))
	}

	resp, err := client.persistentSubscriptionClient.GetInfo(ctx, getInfoReq, callOptions...)
	if err != nil {
		return nil, client.inner.handleError(handle, headers, trailers, err)
	}

	info, err := subscriptionInfoFromWire(resp.SubscriptionInfo)

	if err != nil {
		return nil, err
	}

	return info, nil
}

func (client *persistentClient) replayParkedMessages(
	parent context.Context,
	conf *Configuration,
	handle *connectionHandle,
	streamName *string,
	groupName string,
	options ReplayParkedMessagesOptions,
) error {
	replayOptions := &persistent.ReplayParkedReq_Options{}

	if streamName == nil {
		replayOptions.StreamOption = &persistent.ReplayParkedReq_Options_All{All: &shared.Empty{}}
	} else {
		replayOptions.StreamOption = &persistent.ReplayParkedReq_Options_StreamIdentifier{
			StreamIdentifier: &shared.StreamIdentifier{
				StreamName: []byte(*streamName),
			},
		}
	}

	replayOptions.GroupName = groupName

	if options.StopAt == 0 {
		replayOptions.StopAtOption = &persistent.ReplayParkedReq_Options_NoLimit{NoLimit: &shared.Empty{}}
	} else {
		replayOptions.StopAtOption = &persistent.ReplayParkedReq_Options_StopAt{StopAt: int64(options.StopAt)}
	}

	replayReq := &persistent.ReplayParkedReq{Options: replayOptions}

	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	callOptions, ctx, cancel := configureGrpcCall(parent, conf, options, callOptions)
	defer cancel()
	if options.Authenticated != nil {
		callOptions = append(callOptions, grpc.PerRPCCredentials(basicAuth{
			username: options.Authenticated.Login,
			password: options.Authenticated.Password,
		}))
	}
	_, err := client.persistentSubscriptionClient.ReplayParked(ctx, replayReq, callOptions...)

	if err != nil {
		return client.inner.handleError(handle, headers, trailers, err)
	}

	return nil
}

func (client *persistentClient) restartSubsystem(
	parent context.Context,
	conf *Configuration,
	handle *connectionHandle,
	options RestartPersistentSubscriptionSubsystemOptions,
) error {
	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	callOptions, ctx, cancel := configureGrpcCall(parent, conf, options, callOptions)
	defer cancel()
	if options.Authenticated != nil {
		callOptions = append(callOptions, grpc.PerRPCCredentials(basicAuth{
			username: options.Authenticated.Login,
			password: options.Authenticated.Password,
		}))
	}
	_, err := client.persistentSubscriptionClient.RestartSubsystem(ctx, &shared.Empty{}, callOptions...)

	if err != nil {
		return client.inner.handleError(handle, headers, trailers, err)
	}

	return nil
}

func subscriptionInfoFromWire(wire *persistent.SubscriptionInfo) (*PersistentSubscriptionInfo, error) {
	stats := PersistentSubscriptionStats{}
	stats.AveragePerSecond = int64(wire.AveragePerSecond)
	stats.TotalItems = wire.TotalItems
	stats.CountSinceLastMeasurement = wire.CountSinceLastMeasurement
	stats.ReadBufferCount = int64(wire.ReadBufferCount)
	stats.LiveBufferCount = wire.LiveBufferCount
	stats.RetryBufferCount = int64(wire.RetryBufferCount)
	stats.TotalInFlightMessages = int64(wire.TotalInFlightMessages)
	stats.OutstandingMessagesCount = int64(wire.OutstandingMessagesCount)
	stats.ParkedMessagesCount = wire.ParkedMessageCount

	settings := PersistentSubscriptionSettings{}

	if wire.LastCheckpointedEventPosition != "" {
		if wire.EventSource == "$all" {
			lastPos, err := parsePosition(wire.LastCheckpointedEventPosition)
			if err != nil {
				return nil, err
			}

			stats.LastCheckpointedPosition = lastPos
		} else {
			lastRev, err := parseEventRevision(wire.LastCheckpointedEventPosition)
			if err != nil {
				return nil, err
			}

			stats.LastCheckpointedEventRevision = new(uint64)
			*stats.LastKnownEventRevision = lastRev
		}
	}

	if wire.LastKnownEventPosition != "" {
		if wire.EventSource == "$all" {
			lastPos, err := parsePosition(wire.LastKnownEventPosition)
			if err != nil {
				return nil, err
			}

			stats.LastKnownPosition = lastPos
		} else {
			lastRev, err := parseEventRevision(wire.LastKnownEventPosition)
			if err != nil {
				return nil, err
			}

			stats.LastKnownEventRevision = new(uint64)
			*stats.LastKnownEventRevision = lastRev
		}
	}

	startFrom, err := parseStreamPosition(wire.StartFrom)

	if err != nil {
		return nil, &Error{
			code: ErrorCodeParsing,
			err:  fmt.Errorf("error when parsing StartFrom"),
		}
	}

	settings.StartFrom = startFrom
	settings.ResolveLinkTos = wire.ResolveLinkTos
	settings.MessageTimeout = wire.MessageTimeoutMilliseconds
	settings.ExtraStatistics = wire.ExtraStatistics
	settings.MaxRetryCount = wire.MaxRetryCount
	settings.LiveBufferSize = wire.LiveBufferSize
	settings.HistoryBufferSize = wire.BufferSize
	settings.ReadBatchSize = wire.ReadBatchSize
	settings.CheckpointAfter = wire.CheckPointAfterMilliseconds
	settings.CheckpointLowerBound = wire.MinCheckPointCount
	settings.CheckpointUpperBound = wire.MaxCheckPointCount
	settings.MaxSubscriberCount = wire.MaxSubscriberCount
	settings.ConsumerStrategyName = ConsumerStrategy(wire.NamedConsumerStrategy)

	var connections []PersistentSubscriptionConnectionInfo
	for _, connWire := range wire.Connections {
		var stats []PersistentSubscriptionMeasurement
		for _, statsWire := range connWire.ObservedMeasurements {
			stats = append(stats, PersistentSubscriptionMeasurement{
				Key:   statsWire.Key,
				Value: statsWire.Value,
			})
		}

		conn := PersistentSubscriptionConnectionInfo{
			From:                      connWire.From,
			Username:                  connWire.Username,
			AverageItemsPerSecond:     float64(connWire.AverageItemsPerSecond),
			TotalItemsProcessed:       connWire.TotalItems,
			CountSinceLastMeasurement: connWire.CountSinceLastMeasurement,
			AvailableSlots:            int64(connWire.AvailableSlots),
			InFlightMessages:          int64(connWire.InFlightMessages),
			ConnectionName:            connWire.ConnectionName,
			ExtraStatistics:           stats,
		}

		connections = append(connections, conn)
	}

	info := PersistentSubscriptionInfo{
		EventSource: wire.EventSource,
		GroupName:   wire.GroupName,
		Status:      wire.Status,
		Connections: connections,
		Settings:    &settings,
		Stats:       &stats,
	}

	return &info, nil
}

func newPersistentClient(inner *grpcClient, client persistent.PersistentSubscriptionsClient) persistentClient {
	return persistentClient{
		inner:                        inner,
		persistentSubscriptionClient: client,
	}
}
