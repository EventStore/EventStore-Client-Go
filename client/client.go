package client

import (
	"context"

	"github.com/EventStore/EventStore-Client-Go/connection"
	"github.com/EventStore/EventStore-Client-Go/event_streams"
	"github.com/EventStore/EventStore-Client-Go/persistent"
	"github.com/EventStore/EventStore-Client-Go/projections"
	persistentProto "github.com/EventStore/EventStore-Client-Go/protos/persistent"
	projectionsProto "github.com/EventStore/EventStore-Client-Go/protos/projections"
)

type Configuration = connection.Configuration

func ParseConnectionString(str string) (*connection.Configuration, error) {
	return connection.ParseConnectionString(str)
}

// Client ...
type Client struct {
	grpcClient                connection.GrpcClient
	Config                    *connection.Configuration
	persistentClientFactory   persistent.ClientFactory
	projectionClientFactory   projections.ClientFactory
	eventStreamsClientFactory event_streams.ClientFactory
}

// NewClient ...
func NewClient(configuration *connection.Configuration) (*Client, error) {
	grpcClient := connection.NewGrpcClient(*configuration)
	return &Client{
		grpcClient:                grpcClient,
		Config:                    configuration,
		persistentClientFactory:   persistent.ClientFactoryImpl{},
		projectionClientFactory:   projections.ClientFactoryImpl{},
		eventStreamsClientFactory: event_streams.ClientFactoryImpl{},
	}, nil
}

// Close ...
func (client *Client) Close() error {
	client.grpcClient.Close()
	return nil
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

func (client *Client) CreateProjection(ctx context.Context, options projections.CreateOptionsRequest) error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}

	projectionsClient := client.projectionClientFactory.CreateClient(client.grpcClient,
		projectionsProto.NewProjectionsClient(handle.Connection()))

	return projectionsClient.CreateProjection(ctx, handle, options)
}

func (client *Client) UpdateProjection(ctx context.Context, options projections.UpdateOptionsRequest) error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}

	projectionsClient := client.projectionClientFactory.CreateClient(client.grpcClient,
		projectionsProto.NewProjectionsClient(handle.Connection()))

	return projectionsClient.UpdateProjection(ctx, handle, options)
}

func (client *Client) AbortProjection(ctx context.Context, options projections.AbortOptionsRequest) error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}

	projectionsClient := client.projectionClientFactory.CreateClient(client.grpcClient,
		projectionsProto.NewProjectionsClient(handle.Connection()))

	return projectionsClient.AbortProjection(ctx, handle, options)
}

func (client *Client) DisableProjection(ctx context.Context, options projections.DisableOptionsRequest) error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}

	projectionsClient := client.projectionClientFactory.CreateClient(client.grpcClient,
		projectionsProto.NewProjectionsClient(handle.Connection()))

	return projectionsClient.DisableProjection(ctx, handle, options)
}

func (client *Client) ResetProjection(ctx context.Context, options projections.ResetOptionsRequest) error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}

	projectionsClient := client.projectionClientFactory.CreateClient(client.grpcClient,
		projectionsProto.NewProjectionsClient(handle.Connection()))

	return projectionsClient.ResetProjection(ctx, handle, options)
}

func (client *Client) DeleteProjection(ctx context.Context, options projections.DeleteOptionsRequest) error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}

	projectionsClient := client.projectionClientFactory.CreateClient(client.grpcClient,
		projectionsProto.NewProjectionsClient(handle.Connection()))

	return projectionsClient.DeleteProjection(ctx, handle, options)
}

func (client *Client) EnableProjection(ctx context.Context, options projections.EnableOptionsRequest) error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}

	projectionsClient := client.projectionClientFactory.CreateClient(client.grpcClient,
		projectionsProto.NewProjectionsClient(handle.Connection()))

	return projectionsClient.EnableProjection(ctx, handle, options)
}

func (client *Client) RestartProjectionsSubsystem(ctx context.Context) error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}

	projectionsClient := client.projectionClientFactory.CreateClient(client.grpcClient,
		projectionsProto.NewProjectionsClient(handle.Connection()))

	return projectionsClient.RestartProjectionsSubsystem(ctx, handle)
}

func (client *Client) GetProjectionState(
	ctx context.Context,
	options projections.StateOptionsRequest) (projections.StateResponse, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}

	projectionsClient := client.projectionClientFactory.CreateClient(client.grpcClient,
		projectionsProto.NewProjectionsClient(handle.Connection()))

	return projectionsClient.GetProjectionState(ctx, handle, options)
}

func (client *Client) GetProjectionResult(
	ctx context.Context,
	options projections.ResultOptionsRequest) (projections.ResultResponse, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}

	projectionsClient := client.projectionClientFactory.CreateClient(client.grpcClient,
		projectionsProto.NewProjectionsClient(handle.Connection()))

	return projectionsClient.GetProjectionResult(ctx, handle, options)
}

func (client *Client) GetProjectionStatistics(
	ctx context.Context,
	options projections.StatisticsOptionsRequest) (projections.StatisticsClientSync, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}

	projectionsClient := client.projectionClientFactory.CreateClient(client.grpcClient,
		projectionsProto.NewProjectionsClient(handle.Connection()))

	return projectionsClient.GetProjectionStatistics(ctx, handle, options)
}

func (client *Client) ListAllProjections(
	ctx context.Context) ([]projections.StatisticsClientResponse, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}

	projectionsClient := client.projectionClientFactory.CreateClient(client.grpcClient,
		projectionsProto.NewProjectionsClient(handle.Connection()))

	return projectionsClient.ListAllProjections(ctx, handle)
}

func (client *Client) ListContinuousProjections(
	ctx context.Context) ([]projections.StatisticsClientResponse, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}

	projectionsClient := client.projectionClientFactory.CreateClient(client.grpcClient,
		projectionsProto.NewProjectionsClient(handle.Connection()))

	return projectionsClient.ListContinuousProjections(ctx, handle)
}

func (client *Client) ListOneTimeProjections(
	ctx context.Context) ([]projections.StatisticsClientResponse, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}

	projectionsClient := client.projectionClientFactory.CreateClient(client.grpcClient,
		projectionsProto.NewProjectionsClient(handle.Connection()))

	return projectionsClient.ListOneTimeProjections(ctx, handle)
}

//func readInternal(
//	ctx context.Context,
//	client connection.GrpcClient,
//	handle connection.ConnectionHandle,
//	streamsClient api.StreamsClient,
//	readRequest *api.ReadReq,
//) (*ReadStream, error) {
//	var headers, trailers metadata.MD
//	ctx, cancel := context.WithCancel(ctx)
//	result, err := streamsClient.Read(ctx, readRequest, grpc.Header(&headers), grpc.Trailer(&trailers))
//	if err != nil {
//		defer cancel()
//		err = client.HandleError(handle, headers, trailers, err)
//		return nil, fmt.Errorf("failed to construct read stream. Reason: %v", err)
//	}
//
//	params := ReadStreamParams{
//		client:   client,
//		handle:   handle,
//		cancel:   cancel,
//		inner:    result,
//		headers:  headers,
//		trailers: trailers,
//	}
//
//	stream := NewReadStream(params)
//
//	return stream, nil
//}
