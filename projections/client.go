package projections

//go:generate mockgen -source=client.go -destination=client_mock.go -package=projections

import (
	"context"

	"github.com/EventStore/EventStore-Client-Go/connection"
	"github.com/EventStore/EventStore-Client-Go/errors"
)

type Client interface {
	CreateProjection(ctx context.Context,
		handle connection.ConnectionHandle,
		options CreateOptionsRequest) errors.Error
	UpdateProjection(ctx context.Context,
		handle connection.ConnectionHandle,
		options UpdateOptionsRequest) errors.Error
	DeleteProjection(ctx context.Context,
		handle connection.ConnectionHandle,
		options DeleteOptionsRequest) errors.Error
	GetProjectionStatistics(ctx context.Context,
		handle connection.ConnectionHandle,
		options StatisticsOptionsRequest) (StatisticsClientSync, errors.Error)
	AbortProjection(ctx context.Context,
		handle connection.ConnectionHandle,
		options AbortOptionsRequest) errors.Error
	DisableProjection(ctx context.Context,
		handle connection.ConnectionHandle,
		options DisableOptionsRequest) errors.Error
	EnableProjection(ctx context.Context,
		handle connection.ConnectionHandle,
		options EnableOptionsRequest) errors.Error
	ResetProjection(ctx context.Context,
		handle connection.ConnectionHandle,
		options ResetOptionsRequest) errors.Error
	GetProjectionState(ctx context.Context,
		handle connection.ConnectionHandle,
		options StateOptionsRequest) (StateResponse, errors.Error)
	GetProjectionResult(ctx context.Context,
		handle connection.ConnectionHandle,
		options ResultOptionsRequest) (ResultResponse, errors.Error)
	RestartProjectionsSubsystem(ctx context.Context, handle connection.ConnectionHandle) errors.Error
	ListAllProjections(ctx context.Context,
		handle connection.ConnectionHandle) ([]StatisticsClientResponse, errors.Error)
	ListContinuousProjections(ctx context.Context,
		handle connection.ConnectionHandle) ([]StatisticsClientResponse, errors.Error)
	ListOneTimeProjections(
		ctx context.Context,
		handle connection.ConnectionHandle) ([]StatisticsClientResponse, errors.Error)
}
