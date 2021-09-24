package projections

import (
	"context"
	"io"

	"github.com/pivonroll/EventStore-Client-Go/errors"
	"github.com/pivonroll/EventStore-Client-Go/protos/shared"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/pivonroll/EventStore-Client-Go/connection"
)

type ClientImpl struct {
	grpcClient                   connection.GrpcClient
	grpcProjectionsClientFactory grpcProjectionsClientFactory
}

const FailedToCreateProjectionErr errors.ErrorCode = "FailedToCreateProjectionErr"

func (client *ClientImpl) CreateProjection(
	ctx context.Context,
	options CreateOptionsRequest) errors.Error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}

	projectionsClient := client.grpcProjectionsClientFactory.Create(handle.Connection())

	var headers, trailers metadata.MD
	_, protoErr := projectionsClient.Create(ctx, options.Build(), grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		err := client.grpcClient.HandleError(handle, headers, trailers, protoErr, FailedToCreateProjectionErr)
		return err
	}

	return nil
}

const FailedToUpdateProjectionErr errors.ErrorCode = "FailedToUpdateProjectionErr"

func (client *ClientImpl) UpdateProjection(
	ctx context.Context,
	options UpdateOptionsRequest) errors.Error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}

	projectionsClient := client.grpcProjectionsClientFactory.Create(handle.Connection())

	var headers, trailers metadata.MD
	_, protoErr := projectionsClient.Update(ctx, options.Build(), grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		err := client.grpcClient.HandleError(handle, headers, trailers, protoErr, FailedToUpdateProjectionErr)
		return err
	}

	return nil
}

const FailedToDeleteProjectionErr errors.ErrorCode = "FailedToDeleteProjectionErr"

func (client *ClientImpl) DeleteProjection(
	ctx context.Context,
	options DeleteOptionsRequest) errors.Error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}

	projectionsClient := client.grpcProjectionsClientFactory.Create(handle.Connection())

	var headers, trailers metadata.MD
	_, protoErr := projectionsClient.Delete(ctx, options.Build(), grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		err := client.grpcClient.HandleError(handle, headers, trailers, protoErr, FailedToDeleteProjectionErr)
		return err
	}

	return nil
}

const FailedToFetchProjectionStatisticsErr errors.ErrorCode = "FailedToFetchProjectionStatisticsErr"

func (client *ClientImpl) GetProjectionStatistics(
	ctx context.Context,
	options StatisticsOptionsRequest) (StatisticsClientSync, errors.Error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}

	projectionsClient := client.grpcProjectionsClientFactory.Create(handle.Connection())

	var headers, trailers metadata.MD

	statisticsClient, protoErr := projectionsClient.Statistics(ctx, options.Build(),
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		err := client.grpcClient.HandleError(handle, headers, trailers, protoErr, FailedToFetchProjectionStatisticsErr)
		return nil, err
	}

	return newStatisticsClientSyncImpl(statisticsClient), nil
}

const FailedToDisableProjectionErr errors.ErrorCode = "FailedToDisableProjectionErr"

func (client *ClientImpl) DisableProjection(
	ctx context.Context,
	options DisableOptionsRequest) errors.Error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}

	projectionsClient := client.grpcProjectionsClientFactory.Create(handle.Connection())

	var headers, trailers metadata.MD

	_, protoErr := projectionsClient.Disable(ctx, options.Build(), grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		err := client.grpcClient.HandleError(handle, headers, trailers, protoErr, FailedToDisableProjectionErr)
		return err
	}

	return nil
}

const FailedToAbortProjectionErr errors.ErrorCode = "FailedToAbortProjectionErr"

func (client *ClientImpl) AbortProjection(
	ctx context.Context,
	options AbortOptionsRequest) errors.Error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}

	projectionsClient := client.grpcProjectionsClientFactory.Create(handle.Connection())

	var headers, trailers metadata.MD
	_, protoErr := projectionsClient.Disable(ctx, options.Build(), grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		err := client.grpcClient.HandleError(handle, headers, trailers, protoErr, FailedToAbortProjectionErr)
		return err
	}

	return nil
}

const FailedToEnableProjectionErr errors.ErrorCode = "FailedToEnableProjectionErr"

func (client *ClientImpl) EnableProjection(
	ctx context.Context,
	options EnableOptionsRequest) errors.Error {

	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}

	projectionsClient := client.grpcProjectionsClientFactory.Create(handle.Connection())

	var headers, trailers metadata.MD
	_, protoErr := projectionsClient.Enable(ctx, options.Build(), grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		err := client.grpcClient.HandleError(handle, headers, trailers, protoErr, FailedToEnableProjectionErr)
		return err
	}

	return nil
}

const FailedToResetProjectionErr errors.ErrorCode = "FailedToResetProjectionErr"

func (client *ClientImpl) ResetProjection(
	ctx context.Context,
	options ResetOptionsRequest) errors.Error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}

	projectionsClient := client.grpcProjectionsClientFactory.Create(handle.Connection())

	var headers, trailers metadata.MD
	_, protoErr := projectionsClient.Reset(ctx, options.Build(), grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		err := client.grpcClient.HandleError(handle, headers, trailers, protoErr, FailedToResetProjectionErr)
		return err
	}

	return nil
}

const FailedToGetProjectionStateErr errors.ErrorCode = "FailedToGetProjectionStateErr"

func (client *ClientImpl) GetProjectionState(
	ctx context.Context,
	options StateOptionsRequest) (StateResponse, errors.Error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}

	projectionsClient := client.grpcProjectionsClientFactory.Create(handle.Connection())

	var headers, trailers metadata.MD
	result, protoErr := projectionsClient.State(ctx, options.Build(), grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		err := client.grpcClient.HandleError(handle, headers, trailers, protoErr, FailedToGetProjectionStateErr)
		return nil, err
	}

	return newStateResponse(result), nil
}

const FailedToGetProjectionResultErr errors.ErrorCode = "FailedToGetProjectionResultErr"

func (client *ClientImpl) GetProjectionResult(
	ctx context.Context,
	options ResultOptionsRequest) (ResultResponse, errors.Error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}

	projectionsClient := client.grpcProjectionsClientFactory.Create(handle.Connection())

	var headers, trailers metadata.MD
	result, protoErr := projectionsClient.Result(ctx, options.Build(), grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		err := client.grpcClient.HandleError(handle, headers, trailers, protoErr, FailedToGetProjectionResultErr)
		return nil, err
	}

	return newResultResponse(result), nil
}

const FailedToRestartProjectionsSubsystemErr errors.ErrorCode = "FailedToRestartProjectionsSubsystemErr"

func (client *ClientImpl) RestartProjectionsSubsystem(ctx context.Context) errors.Error {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return err
	}

	projectionsClient := client.grpcProjectionsClientFactory.Create(handle.Connection())

	var headers, trailers metadata.MD
	_, protoErr := projectionsClient.RestartSubsystem(ctx, &shared.Empty{},
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		err := client.grpcClient.HandleError(handle, headers, trailers, protoErr,
			FailedToRestartProjectionsSubsystemErr)
		return err
	}

	return nil
}

const (
	FailedToReadStatistics errors.ErrorCode = "FailedToReadStatistics"
)

func (client *ClientImpl) ListAllProjections(
	ctx context.Context) ([]StatisticsClientResponse, errors.Error) {

	options := StatisticsOptionsRequest{}
	options.SetMode(StatisticsOptionsRequestModeAll{})

	statisticsClient, err := client.GetProjectionStatistics(ctx, options)
	if err != nil {
		return nil, err
	}

	var result []StatisticsClientResponse

	for {
		statisticsResult, protoErr := statisticsClient.Read()
		if protoErr != nil {
			if protoErr == io.EOF {
				break
			}
			return nil, errors.NewError(FailedToReadStatistics, protoErr)
		}

		result = append(result, statisticsResult)
	}

	return result, nil
}

func (client *ClientImpl) ListContinuousProjections(
	ctx context.Context) ([]StatisticsClientResponse, errors.Error) {
	options := StatisticsOptionsRequest{}
	options.SetMode(StatisticsOptionsRequestModeContinuous{})

	statisticsClient, err := client.GetProjectionStatistics(ctx, options)
	if err != nil {
		return nil, err
	}

	var result []StatisticsClientResponse

	for {
		statisticsResult, protoErr := statisticsClient.Read()
		if protoErr != nil {
			if protoErr == io.EOF {
				break
			}
			return nil, errors.NewError(FailedToReadStatistics, protoErr)
		}

		result = append(result, statisticsResult)
	}

	return result, nil
}

func (client *ClientImpl) ListOneTimeProjections(
	ctx context.Context) ([]StatisticsClientResponse, errors.Error) {
	options := StatisticsOptionsRequest{}
	options.SetMode(StatisticsOptionsRequestModeOneTime{})

	statisticsClient, err := client.GetProjectionStatistics(ctx, options)
	if err != nil {
		return nil, err
	}

	var result []StatisticsClientResponse

	for {
		statisticsResult, protoErr := statisticsClient.Read()
		if protoErr != nil {
			if protoErr == io.EOF {
				break
			}
			return nil, errors.NewError(FailedToReadStatistics, protoErr)
		}

		result = append(result, statisticsResult)
	}

	return result, nil
}

func newClientImpl(
	grpcClient connection.GrpcClient,
	grpcProjectionsClientFactory grpcProjectionsClientFactory) *ClientImpl {
	return &ClientImpl{
		grpcProjectionsClientFactory: grpcProjectionsClientFactory,
		grpcClient:                   grpcClient,
	}
}
