package client

import (
	"context"

	"github.com/EventStore/EventStore-Client-Go/connection"
	"github.com/EventStore/EventStore-Client-Go/errors"
	"github.com/EventStore/EventStore-Client-Go/event_streams"
	"github.com/EventStore/EventStore-Client-Go/persistent"
	"github.com/EventStore/EventStore-Client-Go/projections"
	projectionsProto "github.com/EventStore/EventStore-Client-Go/protos/projections"
	"github.com/EventStore/EventStore-Client-Go/user_management"
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
	userManagementFactory     user_management.ClientFactory
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
		userManagementFactory:     user_management.ClientFactoryImpl{},
	}, nil
}

// Close ...
func (client *Client) Close() error {
	client.grpcClient.Close()
	return nil
}

func (client *Client) CreateProjection(
	ctx context.Context,
	options projections.CreateOptionsRequest) errors.Error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}

	projectionsClient := client.projectionClientFactory.CreateClient(client.grpcClient,
		projectionsProto.NewProjectionsClient(handle.Connection()))

	return projectionsClient.CreateProjection(ctx, handle, options)
}

func (client *Client) UpdateProjection(
	ctx context.Context,
	options projections.UpdateOptionsRequest) errors.Error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}

	projectionsClient := client.projectionClientFactory.CreateClient(client.grpcClient,
		projectionsProto.NewProjectionsClient(handle.Connection()))

	return projectionsClient.UpdateProjection(ctx, handle, options)
}

func (client *Client) AbortProjection(
	ctx context.Context,
	options projections.AbortOptionsRequest) errors.Error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}

	projectionsClient := client.projectionClientFactory.CreateClient(client.grpcClient,
		projectionsProto.NewProjectionsClient(handle.Connection()))

	return projectionsClient.AbortProjection(ctx, handle, options)
}

func (client *Client) DisableProjection(
	ctx context.Context,
	options projections.DisableOptionsRequest) errors.Error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}

	projectionsClient := client.projectionClientFactory.CreateClient(client.grpcClient,
		projectionsProto.NewProjectionsClient(handle.Connection()))

	return projectionsClient.DisableProjection(ctx, handle, options)
}

func (client *Client) ResetProjection(
	ctx context.Context,
	options projections.ResetOptionsRequest) errors.Error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}

	projectionsClient := client.projectionClientFactory.CreateClient(client.grpcClient,
		projectionsProto.NewProjectionsClient(handle.Connection()))

	return projectionsClient.ResetProjection(ctx, handle, options)
}

func (client *Client) DeleteProjection(
	ctx context.Context,
	options projections.DeleteOptionsRequest) errors.Error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}

	projectionsClient := client.projectionClientFactory.CreateClient(client.grpcClient,
		projectionsProto.NewProjectionsClient(handle.Connection()))

	return projectionsClient.DeleteProjection(ctx, handle, options)
}

func (client *Client) EnableProjection(
	ctx context.Context,
	options projections.EnableOptionsRequest) errors.Error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}

	projectionsClient := client.projectionClientFactory.CreateClient(client.grpcClient,
		projectionsProto.NewProjectionsClient(handle.Connection()))

	return projectionsClient.EnableProjection(ctx, handle, options)
}

func (client *Client) RestartProjectionsSubsystem(ctx context.Context) errors.Error {
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
	options projections.StateOptionsRequest) (projections.StateResponse, errors.Error) {
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
	options projections.ResultOptionsRequest) (projections.ResultResponse, errors.Error) {
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
	options projections.StatisticsOptionsRequest) (projections.StatisticsClientSync, errors.Error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}

	projectionsClient := client.projectionClientFactory.CreateClient(client.grpcClient,
		projectionsProto.NewProjectionsClient(handle.Connection()))

	return projectionsClient.GetProjectionStatistics(ctx, handle, options)
}

func (client *Client) ListAllProjections(
	ctx context.Context) ([]projections.StatisticsClientResponse, errors.Error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}

	projectionsClient := client.projectionClientFactory.CreateClient(client.grpcClient,
		projectionsProto.NewProjectionsClient(handle.Connection()))

	return projectionsClient.ListAllProjections(ctx, handle)
}

func (client *Client) ListContinuousProjections(
	ctx context.Context) ([]projections.StatisticsClientResponse, errors.Error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}

	projectionsClient := client.projectionClientFactory.CreateClient(client.grpcClient,
		projectionsProto.NewProjectionsClient(handle.Connection()))

	return projectionsClient.ListContinuousProjections(ctx, handle)
}

func (client *Client) ListOneTimeProjections(
	ctx context.Context) ([]projections.StatisticsClientResponse, errors.Error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}

	projectionsClient := client.projectionClientFactory.CreateClient(client.grpcClient,
		projectionsProto.NewProjectionsClient(handle.Connection()))

	return projectionsClient.ListOneTimeProjections(ctx, handle)
}

func (client *Client) UserManagement() user_management.Client {
	return client.userManagementFactory.Create(client.grpcClient)
}

func (client *Client) EventStreams() event_streams.Client {
	return client.eventStreamsClientFactory.CreateClient(client.grpcClient)
}

func (client *Client) PersistentSubscriptions() persistent.Client {
	return client.persistentClientFactory.CreateClient(client.grpcClient)
}
