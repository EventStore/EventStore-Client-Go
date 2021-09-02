package projections

import (
	"context"
	"errors"

	"github.com/EventStore/EventStore-Client-Go/connection"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/EventStore/EventStore-Client-Go/protos/projections"

	"github.com/EventStore/EventStore-Client-Go/protos/shared"
)

type Client interface {
	CreateProjection(ctx context.Context, handle connection.ConnectionHandle, options CreateOptionsRequest) error
	UpdateProjection(ctx context.Context, handle connection.ConnectionHandle, options UpdateOptionsRequest) error
	DeleteProjection(ctx context.Context, handle connection.ConnectionHandle, options DeleteOptionsRequest) error
	GetProjectionStatistics(ctx context.Context,
		handle connection.ConnectionHandle,
		options StatisticsOptionsRequest) (StatisticsClientSync, error)
	AbortProjection(ctx context.Context, handle connection.ConnectionHandle, options AbortOptionsRequest) error
	DisableProjection(ctx context.Context, handle connection.ConnectionHandle, options DisableOptionsRequest) error
	EnableProjection(ctx context.Context, handle connection.ConnectionHandle, options EnableOptionsRequest) error
	ResetProjection(ctx context.Context, handle connection.ConnectionHandle, options ResetOptionsRequest) error
	GetProjectionState(ctx context.Context,
		handle connection.ConnectionHandle,
		options StateOptionsRequest) (StateResponse, error)
	GetProjectionResult(ctx context.Context,
		handle connection.ConnectionHandle,
		options ResultOptionsRequest) (ResultResponse, error)
	RestartProjectionsSubsystem(ctx context.Context, handle connection.ConnectionHandle) error
}

type ClientImpl struct {
	grpcClient        connection.GrpcClient
	projectionsClient projections.ProjectionsClient
}

const FailedToCreateProjectionErr = "FailedToCreateProjectionErr"

func (client *ClientImpl) CreateProjection(
	ctx context.Context,
	handle connection.ConnectionHandle,
	options CreateOptionsRequest) error {

	var headers, trailers metadata.MD
	_, err := client.projectionsClient.Create(ctx, options.Build(), grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return errors.New(FailedToCreateProjectionErr)
	}

	return nil
}

const FailedToUpdateProjectionErr = "FailedToUpdateProjectionErr"

func (client *ClientImpl) UpdateProjection(
	ctx context.Context,
	handle connection.ConnectionHandle,
	options UpdateOptionsRequest) error {

	var headers, trailers metadata.MD
	_, err := client.projectionsClient.Update(ctx, options.Build(), grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return errors.New(FailedToUpdateProjectionErr)
	}

	return nil
}

const FailedToDeleteProjectionErr = "FailedToDeleteProjectionErr"

func (client *ClientImpl) DeleteProjection(
	ctx context.Context,
	handle connection.ConnectionHandle,
	options DeleteOptionsRequest) error {

	var headers, trailers metadata.MD
	_, err := client.projectionsClient.Delete(ctx, options.Build(), grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return errors.New(FailedToDeleteProjectionErr)
	}

	return nil
}

const FailedToFetchProjectionStatisticsErr = "FailedToFetchProjectionStatisticsErr"

func (client *ClientImpl) GetProjectionStatistics(
	ctx context.Context,
	handle connection.ConnectionHandle,
	options StatisticsOptionsRequest) (StatisticsClientSync, error) {

	var headers, trailers metadata.MD

	statisticsClient, err := client.projectionsClient.Statistics(ctx, options.Build(),
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return nil, errors.New(FailedToFetchProjectionStatisticsErr)
	}

	return newStatisticsClientSyncImpl(statisticsClient), nil
}

const FailedToDisableProjectionErr = "FailedToDisableProjectionErr"

func (client *ClientImpl) DisableProjection(
	ctx context.Context,
	handle connection.ConnectionHandle,
	options DisableOptionsRequest) error {
	var headers, trailers metadata.MD

	_, err := client.projectionsClient.Disable(ctx, options.Build(), grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return errors.New(FailedToDisableProjectionErr)
	}

	return nil
}

const FailedToAbortProjectionErr = "FailedToAbortProjectionErr"

func (client *ClientImpl) AbortProjection(
	ctx context.Context,
	handle connection.ConnectionHandle,
	options AbortOptionsRequest) error {

	var headers, trailers metadata.MD
	_, err := client.projectionsClient.Disable(ctx, options.Build(), grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return errors.New(FailedToAbortProjectionErr)
	}

	return nil
}

const FailedToEnableProjectionErr = "FailedToEnableProjectionErr"

func (client *ClientImpl) EnableProjection(
	ctx context.Context,
	handle connection.ConnectionHandle,
	options EnableOptionsRequest) error {

	var headers, trailers metadata.MD
	_, err := client.projectionsClient.Enable(ctx, options.Build(), grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return errors.New(FailedToEnableProjectionErr)
	}

	return nil
}

const FailedToResetProjectionErr = "FailedToResetProjectionErr"

func (client *ClientImpl) ResetProjection(
	ctx context.Context,
	handle connection.ConnectionHandle,
	options ResetOptionsRequest) error {

	var headers, trailers metadata.MD
	_, err := client.projectionsClient.Reset(ctx, options.Build(), grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return errors.New(FailedToResetProjectionErr)
	}

	return nil
}

const FailedToGetProjectionStateErr = "FailedToGetProjectionStateErr"

func (client *ClientImpl) GetProjectionState(
	ctx context.Context,
	handle connection.ConnectionHandle,
	options StateOptionsRequest) (StateResponse, error) {

	var headers, trailers metadata.MD
	result, err := client.projectionsClient.State(ctx, options.Build(), grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return nil, errors.New(FailedToGetProjectionStateErr)
	}

	return newStateResponse(result), nil
}

const FailedToGetProjectionResultErr = "FailedToGetProjectionResultErr"

func (client *ClientImpl) GetProjectionResult(
	ctx context.Context,
	handle connection.ConnectionHandle,
	options ResultOptionsRequest) (ResultResponse, error) {

	var headers, trailers metadata.MD
	result, err := client.projectionsClient.Result(ctx, options.Build(), grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return nil, errors.New(FailedToGetProjectionResultErr)
	}

	return newResultResponse(result), nil
}

const FailedToRestartProjectionsSubsystemErr = "FailedToRestartProjectionsSubsystemErr"

func (client *ClientImpl) RestartProjectionsSubsystem(ctx context.Context, handle connection.ConnectionHandle) error {
	var headers, trailers metadata.MD
	_, err := client.projectionsClient.RestartSubsystem(ctx, &shared.Empty{},
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return errors.New(FailedToRestartProjectionsSubsystemErr)
	}

	return nil
}

func newClientImpl(
	grpcClient connection.GrpcClient,
	projectionsClient projections.ProjectionsClient) *ClientImpl {
	return &ClientImpl{
		projectionsClient: projectionsClient,
		grpcClient:        grpcClient,
	}
}
