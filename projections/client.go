package projections

//go:generate mockgen -source=client.go -destination=client_mock.go -package=projections

import (
	"context"

	"github.com/EventStore/EventStore-Client-Go/connection"
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
	ListAllProjections(ctx context.Context, handle connection.ConnectionHandle) ([]StatisticsClientResponse, error)
	ListContinuousProjections(ctx context.Context,
		handle connection.ConnectionHandle) ([]StatisticsClientResponse, error)
	ListOneTimeProjections(
		ctx context.Context,
		handle connection.ConnectionHandle) ([]StatisticsClientResponse, error)
}
