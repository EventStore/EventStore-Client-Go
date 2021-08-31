package projections

import (
	"context"
	"errors"
	"testing"

	"github.com/EventStore/EventStore-Client-Go/protos/shared"

	"google.golang.org/protobuf/types/known/structpb"

	"github.com/stretchr/testify/require"

	"github.com/EventStore/EventStore-Client-Go/protos/projections"
	"github.com/golang/mock/gomock"
)

func TestClientImpl_CreateProjection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	grpcClientMock := projections.NewMockProjectionsClient(ctrl)
	ctx := context.Background()

	options := CreateOptionsRequest{}
	options.SetQuery("some query")
	options.SetMode(CreateConfigModeContinuousOption{
		Name:                "some mode",
		TrackEmittedStreams: true,
	})

	grpcOptions := options.Build()

	t.Run("Success", func(t *testing.T) {
		grpcClientMock.EXPECT().Create(ctx, grpcOptions).Times(1).Return(nil, nil)

		client := ClientImpl{
			projectionsClient: grpcClientMock,
		}

		err := client.CreateProjection(ctx, options)
		require.NoError(t, err)
	})

	t.Run("Error returned from grpc", func(t *testing.T) {
		someError := errors.New("some error")
		grpcClientMock.EXPECT().Create(ctx, grpcOptions).Times(1).Return(nil, someError)

		client := ClientImpl{
			projectionsClient: grpcClientMock,
		}

		err := client.CreateProjection(ctx, options)
		require.EqualError(t, err, someError.Error())
	})
}

func TestClientImpl_UpdateProjection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	grpcClientMock := projections.NewMockProjectionsClient(ctrl)
	ctx := context.Background()

	options := UpdateOptionsRequest{}
	options.SetName("some name").
		SetQuery("some query").
		SetEmitOption(UpdateOptionsEmitOptionEnabled{
			EmitEnabled: true,
		})

	grpcOptions := options.Build()

	t.Run("Success", func(t *testing.T) {
		grpcClientMock.EXPECT().Update(ctx, grpcOptions).Times(1).Return(nil, nil)

		client := ClientImpl{
			projectionsClient: grpcClientMock,
		}

		err := client.UpdateProjection(ctx, options)
		require.NoError(t, err)
	})

	t.Run("Error returned from grpc", func(t *testing.T) {
		someError := errors.New("some error")
		grpcClientMock.EXPECT().Update(ctx, grpcOptions).Times(1).Return(nil, someError)

		client := ClientImpl{
			projectionsClient: grpcClientMock,
		}

		err := client.UpdateProjection(ctx, options)
		require.EqualError(t, err, someError.Error())
	})
}

func TestClientImpl_DeleteProjection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	grpcClientMock := projections.NewMockProjectionsClient(ctrl)
	ctx := context.Background()

	options := DeleteOptionsRequest{}
	options.SetName("some name")

	grpcOptions := options.Build()

	t.Run("Success", func(t *testing.T) {
		grpcClientMock.EXPECT().Delete(ctx, grpcOptions).Times(1).Return(nil, nil)

		client := ClientImpl{
			projectionsClient: grpcClientMock,
		}

		err := client.DeleteProjection(ctx, options)
		require.NoError(t, err)
	})

	t.Run("Error returned from grpc", func(t *testing.T) {
		someError := errors.New("some error")
		grpcClientMock.EXPECT().Delete(ctx, grpcOptions).Times(1).Return(nil, someError)

		client := ClientImpl{
			projectionsClient: grpcClientMock,
		}

		err := client.DeleteProjection(ctx, options)
		require.EqualError(t, err, someError.Error())
	})
}

func TestClientImpl_ProjectionStatistics(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	grpcClientMock := projections.NewMockProjectionsClient(ctrl)
	ctx := context.Background()

	options := StatisticsOptionsRequest{}
	options.SetMode(StatisticsOptionsRequestModeAll{})

	grpcOptions := options.Build()

	t.Run("Success", func(t *testing.T) {
		grpcClientMock.EXPECT().Statistics(ctx, grpcOptions).Times(1).Return(nil, nil)

		client := ClientImpl{
			projectionsClient: grpcClientMock,
		}

		statisticsClient, err := client.ProjectionStatistics(ctx, options)
		require.NotNil(t, statisticsClient)
		require.NoError(t, err)
	})

	t.Run("Error returned from grpc", func(t *testing.T) {
		someError := errors.New("some error")
		grpcClientMock.EXPECT().Statistics(ctx, grpcOptions).Times(1).Return(nil, someError)

		client := ClientImpl{
			projectionsClient: grpcClientMock,
		}

		statisticsClient, err := client.ProjectionStatistics(ctx, options)
		require.Nil(t, statisticsClient)
		require.EqualError(t, err, someError.Error())
	})
}

func TestClientImpl_DisableProjection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	grpcClientMock := projections.NewMockProjectionsClient(ctrl)
	ctx := context.Background()

	options := DisableOptionsRequest{}
	grpcOptions := options.Build()

	t.Run("Success", func(t *testing.T) {
		grpcClientMock.EXPECT().Disable(ctx, grpcOptions).Times(1).Return(nil, nil)

		client := ClientImpl{
			projectionsClient: grpcClientMock,
		}

		err := client.DisableProjection(ctx, options)
		require.NoError(t, err)
	})

	t.Run("Error returned from grpc", func(t *testing.T) {
		someError := errors.New("some error")
		grpcClientMock.EXPECT().Disable(ctx, grpcOptions).Times(1).Return(nil, someError)

		client := ClientImpl{
			projectionsClient: grpcClientMock,
		}

		err := client.DisableProjection(ctx, options)
		require.EqualError(t, err, someError.Error())
	})
}

func TestClientImpl_AbortProjection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	grpcClientMock := projections.NewMockProjectionsClient(ctrl)
	ctx := context.Background()

	options := AbortOptionsRequest{}
	grpcOptions := options.Build()

	t.Run("Success", func(t *testing.T) {
		grpcClientMock.EXPECT().Disable(ctx, grpcOptions).Times(1).Return(nil, nil)

		client := ClientImpl{
			projectionsClient: grpcClientMock,
		}

		err := client.AbortProjection(ctx, options)
		require.NoError(t, err)
	})

	t.Run("Error returned from grpc", func(t *testing.T) {
		someError := errors.New("some error")
		grpcClientMock.EXPECT().Disable(ctx, grpcOptions).Times(1).Return(nil, someError)

		client := ClientImpl{
			projectionsClient: grpcClientMock,
		}

		err := client.AbortProjection(ctx, options)
		require.EqualError(t, err, someError.Error())
	})
}

func TestClientImpl_EnableProjection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	grpcClientMock := projections.NewMockProjectionsClient(ctrl)
	ctx := context.Background()

	options := EnableOptionsRequest{}
	options.SetName("some name")
	grpcOptions := options.Build()

	t.Run("Success", func(t *testing.T) {
		grpcClientMock.EXPECT().Enable(ctx, grpcOptions).Times(1).Return(nil, nil)

		client := ClientImpl{
			projectionsClient: grpcClientMock,
		}

		err := client.EnableProjection(ctx, options)
		require.NoError(t, err)
	})

	t.Run("Error returned from grpc", func(t *testing.T) {
		someError := errors.New("some error")
		grpcClientMock.EXPECT().Enable(ctx, grpcOptions).Times(1).Return(nil, someError)

		client := ClientImpl{
			projectionsClient: grpcClientMock,
		}

		err := client.EnableProjection(ctx, options)
		require.EqualError(t, err, someError.Error())
	})
}

func TestClientImpl_ResetProjection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	grpcClientMock := projections.NewMockProjectionsClient(ctrl)
	ctx := context.Background()

	options := ResetOptionsRequest{}
	options.SetName("some name").SetWriteCheckpoint(true)
	grpcOptions := options.Build()

	t.Run("Success", func(t *testing.T) {
		grpcClientMock.EXPECT().Reset(ctx, grpcOptions).Times(1).Return(nil, nil)

		client := ClientImpl{
			projectionsClient: grpcClientMock,
		}

		err := client.ResetProjection(ctx, options)
		require.NoError(t, err)
	})

	t.Run("Error returned from grpc", func(t *testing.T) {
		someError := errors.New("some error")
		grpcClientMock.EXPECT().Reset(ctx, grpcOptions).Times(1).Return(nil, someError)

		client := ClientImpl{
			projectionsClient: grpcClientMock,
		}

		err := client.ResetProjection(ctx, options)
		require.EqualError(t, err, someError.Error())
	})
}

func TestClientImpl_ProjectionState(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	grpcClientMock := projections.NewMockProjectionsClient(ctrl)
	ctx := context.Background()

	options := StateOptionsRequest{}
	options.SetName("some name").SetPartition("some partition")
	grpcOptions := options.Build()

	response := &projections.StateResp{
		State: &structpb.Value{
			Kind: &structpb.Value_NullValue{},
		},
	}

	t.Run("Success", func(t *testing.T) {
		grpcClientMock.EXPECT().State(ctx, grpcOptions).Times(1).Return(response, nil)

		client := ClientImpl{
			projectionsClient: grpcClientMock,
		}

		expectedStateResponse := newStateResponse(response)

		state, err := client.ProjectionState(ctx, options)
		require.Equal(t, expectedStateResponse, state)
		require.NoError(t, err)
	})

	t.Run("Error returned from grpc", func(t *testing.T) {
		someError := errors.New("some error")
		grpcClientMock.EXPECT().State(ctx, grpcOptions).Times(1).Return(nil, someError)

		client := ClientImpl{
			projectionsClient: grpcClientMock,
		}

		state, err := client.ProjectionState(ctx, options)
		require.Nil(t, state)
		require.EqualError(t, err, someError.Error())
	})
}

func TestClientImpl_ProjectionResult(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	grpcClientMock := projections.NewMockProjectionsClient(ctrl)
	ctx := context.Background()

	options := ResultOptionsRequest{}
	options.SetName("some name").SetPartition("some partition")
	grpcOptions := options.Build()

	response := &projections.ResultResp{
		Result: &structpb.Value{
			Kind: &structpb.Value_NullValue{},
		},
	}

	t.Run("Success", func(t *testing.T) {
		grpcClientMock.EXPECT().Result(ctx, grpcOptions).Times(1).Return(response, nil)

		client := ClientImpl{
			projectionsClient: grpcClientMock,
		}

		expectedStateResponse := newResultResponse(response)

		state, err := client.ProjectionResult(ctx, options)
		require.Equal(t, expectedStateResponse, state)
		require.NoError(t, err)
	})

	t.Run("Error returned from grpc", func(t *testing.T) {
		someError := errors.New("some error")
		grpcClientMock.EXPECT().Result(ctx, grpcOptions).Times(1).Return(nil, someError)

		client := ClientImpl{
			projectionsClient: grpcClientMock,
		}

		state, err := client.ProjectionResult(ctx, options)
		require.Nil(t, state)
		require.EqualError(t, err, someError.Error())
	})
}

func TestClientImpl_RestartProjectionsSubsystem(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	grpcClientMock := projections.NewMockProjectionsClient(ctrl)
	ctx := context.Background()

	t.Run("Success with RestartSubsystem return nil", func(t *testing.T) {
		grpcClientMock.EXPECT().RestartSubsystem(ctx, &shared.Empty{}).Times(1).Return(nil, nil)

		client := ClientImpl{
			projectionsClient: grpcClientMock,
		}

		err := client.RestartProjectionsSubsystem(ctx)
		require.NoError(t, err)
	})

	t.Run("Success with RestartSubsystem return &shared.Empty{}", func(t *testing.T) {
		grpcClientMock.EXPECT().RestartSubsystem(ctx, &shared.Empty{}).Times(1).Return(&shared.Empty{}, nil)

		client := ClientImpl{
			projectionsClient: grpcClientMock,
		}

		err := client.RestartProjectionsSubsystem(ctx)
		require.NoError(t, err)
	})

	t.Run("Error returned from grpc", func(t *testing.T) {
		someError := errors.New("some error")
		grpcClientMock.EXPECT().RestartSubsystem(ctx, &shared.Empty{}).Times(1).Return(nil, someError)

		client := ClientImpl{
			projectionsClient: grpcClientMock,
		}

		err := client.RestartProjectionsSubsystem(ctx)
		require.EqualError(t, err, someError.Error())
	})
}
