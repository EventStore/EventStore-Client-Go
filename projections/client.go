package projections

//go:generate mockgen -source=client.go -destination=client_mock.go -package=projections

import (
	"context"

	"github.com/pivonroll/EventStore-Client-Go/errors"
)

type Client interface {
	CreateProjection(ctx context.Context, options CreateOptionsRequest) errors.Error
	UpdateProjection(ctx context.Context, options UpdateOptionsRequest) errors.Error
	DeleteProjection(ctx context.Context, options DeleteOptionsRequest) errors.Error
	GetProjectionStatistics(ctx context.Context,
		options StatisticsOptionsRequest) (StatisticsClientSync, errors.Error)
	AbortProjection(ctx context.Context, options AbortOptionsRequest) errors.Error
	DisableProjection(ctx context.Context, options DisableOptionsRequest) errors.Error
	EnableProjection(ctx context.Context, options EnableOptionsRequest) errors.Error
	ResetProjection(ctx context.Context, options ResetOptionsRequest) errors.Error
	GetProjectionState(ctx context.Context, options StateOptionsRequest) (StateResponse, errors.Error)
	GetProjectionResult(ctx context.Context, options ResultOptionsRequest) (ResultResponse, errors.Error)
	RestartProjectionsSubsystem(ctx context.Context) errors.Error
	ListAllProjections(ctx context.Context) ([]StatisticsClientResponse, errors.Error)
	ListContinuousProjections(ctx context.Context) ([]StatisticsClientResponse, errors.Error)
	ListOneTimeProjections(ctx context.Context) ([]StatisticsClientResponse, errors.Error)
}
