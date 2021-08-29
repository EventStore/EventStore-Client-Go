package projections

import (
	"context"
	"fmt"

	"github.com/EventStore/EventStore-Client-Go/protos/projections"

	"github.com/EventStore/EventStore-Client-Go/protos/shared"
)

type Client interface {
	CreateProjection(ctx context.Context, options CreateOptionsRequest) error
	UpdateProjection(ctx context.Context, options UpdateOptionsRequest) error
	DeleteProjection(ctx context.Context, options DeleteOptionsRequest) error
	ProjectionStatistics(ctx context.Context, options StatisticsOptionsRequest) (StatisticsClientSync, error)
	DisableProjection(ctx context.Context, options DisableOptionsRequest) error
	EnableProjection(ctx context.Context, options EnableOptionsRequest) error
	ResetProjection(ctx context.Context, options ResetOptionsRequest) error
	ProjectionState(ctx context.Context, options StateOptionsRequest) (interface{}, error)
	ProjectionResult(ctx context.Context, options ResultOptionsRequest) (interface{}, error)
	RestartProjectionsSubsystem(ctx context.Context) error
}

type ClientImpl struct {
	projectionsClient projections.ProjectionsClient
}

func (client *ClientImpl) CreateProjection(ctx context.Context, options CreateOptionsRequest) error {
	_, err := client.projectionsClient.Create(ctx, options.Build())
	if err != nil {
		return err
	}

	return nil
}

func (client *ClientImpl) UpdateProjection(ctx context.Context, options UpdateOptionsRequest) error {
	_, err := client.projectionsClient.Update(ctx, options.Build())
	if err != nil {
		return err
	}

	return nil
}

func (client *ClientImpl) DeleteProjection(ctx context.Context, options DeleteOptionsRequest) error {
	_, err := client.projectionsClient.Delete(ctx, options.Build())
	if err != nil {
		return err
	}

	return nil
}

func (client *ClientImpl) ProjectionStatistics(ctx context.Context, options StatisticsOptionsRequest) (StatisticsClientSync, error) {
	statisticsClient, err := client.projectionsClient.Statistics(ctx, options.Build())
	if err != nil {
		return nil, err
	}

	return newStatisticsClientSyncImpl(statisticsClient), nil
}

func (client *ClientImpl) DisableProjection(ctx context.Context, options DisableOptionsRequest) error {
	_, err := client.projectionsClient.Disable(ctx, options.Build())
	if err != nil {
		return err
	}

	return nil
}

func (client *ClientImpl) AbortProjection(ctx context.Context, options AbortOptionsRequest) error {
	_, err := client.projectionsClient.Disable(ctx, options.Build())
	if err != nil {
		return err
	}

	return nil
}

func (client *ClientImpl) EnableProjection(ctx context.Context, options EnableOptionsRequest) error {
	_, err := client.projectionsClient.Enable(ctx, options.Build())
	if err != nil {
		return err
	}

	return nil
}

func (client *ClientImpl) ResetProjection(ctx context.Context, options ResetOptionsRequest) error {
	_, err := client.projectionsClient.Reset(ctx, options.Build())
	if err != nil {
		return err
	}

	return nil
}

func (client *ClientImpl) ProjectionState(ctx context.Context, options StateOptionsRequest) (interface{}, error) {
	result, err := client.projectionsClient.State(ctx, options.Build())
	if err != nil {
		return nil, err
	}

	fmt.Println("ProjectionState: ", result.State.Kind)

	return result, nil
}

func (client *ClientImpl) ProjectionResult(ctx context.Context, options ResultOptionsRequest) (interface{}, error) {
	result, err := client.projectionsClient.Result(ctx, options.Build())
	if err != nil {
		return nil, err
	}

	fmt.Println("ProjectionResult: ", result.Result.Kind)
	return result, nil
}

func (client *ClientImpl) RestartProjectionsSubsystem(ctx context.Context) error {
	_, err := client.projectionsClient.RestartSubsystem(ctx, &shared.Empty{})
	if err != nil {
		return err
	}

	return nil
}
