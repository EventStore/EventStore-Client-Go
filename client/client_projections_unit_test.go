package client

import (
	"context"
	"errors"
	"testing"

	projectionsProto "github.com/EventStore/EventStore-Client-Go/protos/projections"

	"google.golang.org/grpc"

	"github.com/stretchr/testify/require"

	"github.com/EventStore/EventStore-Client-Go/connection"
	"github.com/EventStore/EventStore-Client-Go/projections"
	"github.com/golang/mock/gomock"
)

func TestClient_CreateProjection(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()
	grpcClient := connection.NewMockGrpcClient(ctrl)
	projectionFactory := projections.NewMockClientFactory(ctrl)
	connectionHandle := connection.NewMockConnectionHandle(ctrl)
	projectionsClient := projections.NewMockClient(ctrl)

	options := projections.CreateOptionsRequest{}
	options.SetQuery("some query")
	options.SetMode(projections.CreateConfigModeOneTimeOption{})

	grpcClientConnection := &grpc.ClientConn{}

	t.Run("grpc client returned an error", func(t *testing.T) {
		errorResult := errors.New("some error")
		grpcClient.EXPECT().GetConnectionHandle().Return(nil, errorResult)

		clientInstance := Client{
			grpcClient: grpcClient,
		}

		err := clientInstance.CreateProjection(ctx, options)
		require.Error(t, err)
		require.EqualError(t, err, errorResult.Error())
	})

	t.Run("Projections client returned error", func(t *testing.T) {
		expectedProtoProjectionsClient := projectionsProto.NewProjectionsClient(grpcClientConnection)

		grpcClient.EXPECT().GetConnectionHandle().Return(connectionHandle, nil)
		connectionHandle.EXPECT().Connection().Return(grpcClientConnection)
		projectionFactory.EXPECT().
			CreateClient(grpcClient, expectedProtoProjectionsClient).Return(projectionsClient)

		errorResult := errors.New("some error")

		projectionsClient.EXPECT().CreateProjection(ctx, connectionHandle, options).Return(errorResult)

		clientInstance := Client{
			grpcClient:              grpcClient,
			projectionClientFactory: projectionFactory,
		}

		err := clientInstance.CreateProjection(ctx, options)
		require.Error(t, err)
		require.EqualError(t, err, errorResult.Error())
	})

	t.Run("Projections client returned success", func(t *testing.T) {
		expectedProtoProjectionsClient := projectionsProto.NewProjectionsClient(grpcClientConnection)

		grpcClient.EXPECT().GetConnectionHandle().Return(connectionHandle, nil)
		connectionHandle.EXPECT().Connection().Return(grpcClientConnection)
		projectionFactory.EXPECT().
			CreateClient(grpcClient, expectedProtoProjectionsClient).Return(projectionsClient)
		projectionsClient.EXPECT().CreateProjection(ctx, connectionHandle, options).Return(nil)

		clientInstance := Client{
			grpcClient:              grpcClient,
			projectionClientFactory: projectionFactory,
		}

		err := clientInstance.CreateProjection(ctx, options)
		require.NoError(t, err)
	})
}

func TestClient_UpdateProjection(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()
	grpcClient := connection.NewMockGrpcClient(ctrl)
	projectionFactory := projections.NewMockClientFactory(ctrl)
	connectionHandle := connection.NewMockConnectionHandle(ctrl)
	projectionsClient := projections.NewMockClient(ctrl)

	options := projections.UpdateOptionsRequest{}
	options.SetQuery("some query").
		SetName("some name").
		SetEmitOption(projections.UpdateOptionsEmitOptionNoEmit{})

	grpcClientConnection := &grpc.ClientConn{}

	t.Run("grpc client returned an error", func(t *testing.T) {
		errorResult := errors.New("some error")
		grpcClient.EXPECT().GetConnectionHandle().Return(nil, errorResult)

		clientInstance := Client{
			grpcClient: grpcClient,
		}

		err := clientInstance.UpdateProjection(ctx, options)
		require.Error(t, err)
		require.EqualError(t, err, errorResult.Error())
	})

	t.Run("Projections client returned error", func(t *testing.T) {
		expectedProtoProjectionsClient := projectionsProto.NewProjectionsClient(grpcClientConnection)

		grpcClient.EXPECT().GetConnectionHandle().Return(connectionHandle, nil)
		connectionHandle.EXPECT().Connection().Return(grpcClientConnection)
		projectionFactory.EXPECT().
			CreateClient(grpcClient, expectedProtoProjectionsClient).Return(projectionsClient)

		errorResult := errors.New("some error")

		projectionsClient.EXPECT().UpdateProjection(ctx, connectionHandle, options).Return(errorResult)

		clientInstance := Client{
			grpcClient:              grpcClient,
			projectionClientFactory: projectionFactory,
		}

		err := clientInstance.UpdateProjection(ctx, options)
		require.Error(t, err)
		require.EqualError(t, err, errorResult.Error())
	})

	t.Run("Projections client factory returned success", func(t *testing.T) {
		expectedProtoProjectionsClient := projectionsProto.NewProjectionsClient(grpcClientConnection)

		grpcClient.EXPECT().GetConnectionHandle().Return(connectionHandle, nil)
		connectionHandle.EXPECT().Connection().Return(grpcClientConnection)
		projectionFactory.EXPECT().
			CreateClient(grpcClient, expectedProtoProjectionsClient).Return(projectionsClient)
		projectionsClient.EXPECT().UpdateProjection(ctx, connectionHandle, options).Return(nil)

		clientInstance := Client{
			grpcClient:              grpcClient,
			projectionClientFactory: projectionFactory,
		}

		err := clientInstance.UpdateProjection(ctx, options)
		require.NoError(t, err)
	})
}

func TestClient_DisableProjection(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()
	grpcClient := connection.NewMockGrpcClient(ctrl)
	projectionFactory := projections.NewMockClientFactory(ctrl)
	connectionHandle := connection.NewMockConnectionHandle(ctrl)
	projectionsClient := projections.NewMockClient(ctrl)

	options := projections.DisableOptionsRequest{}
	options.SetName("some name")

	grpcClientConnection := &grpc.ClientConn{}

	t.Run("grpc client returned an error", func(t *testing.T) {
		errorResult := errors.New("some error")
		grpcClient.EXPECT().GetConnectionHandle().Return(nil, errorResult)

		clientInstance := Client{
			grpcClient: grpcClient,
		}

		err := clientInstance.DisableProjection(ctx, options)
		require.Error(t, err)
		require.EqualError(t, err, errorResult.Error())
	})

	t.Run("Projections client returned error", func(t *testing.T) {
		expectedProtoProjectionsClient := projectionsProto.NewProjectionsClient(grpcClientConnection)

		grpcClient.EXPECT().GetConnectionHandle().Return(connectionHandle, nil)
		connectionHandle.EXPECT().Connection().Return(grpcClientConnection)
		projectionFactory.EXPECT().
			CreateClient(grpcClient, expectedProtoProjectionsClient).Return(projectionsClient)

		errorResult := errors.New("some error")

		projectionsClient.EXPECT().DisableProjection(ctx, connectionHandle, options).Return(errorResult)

		clientInstance := Client{
			grpcClient:              grpcClient,
			projectionClientFactory: projectionFactory,
		}

		err := clientInstance.DisableProjection(ctx, options)
		require.Error(t, err)
		require.EqualError(t, err, errorResult.Error())
	})

	t.Run("Projections client factory returned success", func(t *testing.T) {
		expectedProtoProjectionsClient := projectionsProto.NewProjectionsClient(grpcClientConnection)

		grpcClient.EXPECT().GetConnectionHandle().Return(connectionHandle, nil)
		connectionHandle.EXPECT().Connection().Return(grpcClientConnection)
		projectionFactory.EXPECT().
			CreateClient(grpcClient, expectedProtoProjectionsClient).Return(projectionsClient)
		projectionsClient.EXPECT().DisableProjection(ctx, connectionHandle, options).Return(nil)

		clientInstance := Client{
			grpcClient:              grpcClient,
			projectionClientFactory: projectionFactory,
		}

		err := clientInstance.DisableProjection(ctx, options)
		require.NoError(t, err)
	})
}

func TestClient_EnableProjection(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()
	grpcClient := connection.NewMockGrpcClient(ctrl)
	projectionFactory := projections.NewMockClientFactory(ctrl)
	connectionHandle := connection.NewMockConnectionHandle(ctrl)
	projectionsClient := projections.NewMockClient(ctrl)

	options := projections.EnableOptionsRequest{}
	options.SetName("some name")

	grpcClientConnection := &grpc.ClientConn{}

	t.Run("grpc client returned an error", func(t *testing.T) {
		errorResult := errors.New("some error")
		grpcClient.EXPECT().GetConnectionHandle().Return(nil, errorResult)

		clientInstance := Client{
			grpcClient: grpcClient,
		}

		err := clientInstance.EnableProjection(ctx, options)
		require.Error(t, err)
		require.EqualError(t, err, errorResult.Error())
	})

	t.Run("Projections client returned error", func(t *testing.T) {
		expectedProtoProjectionsClient := projectionsProto.NewProjectionsClient(grpcClientConnection)

		grpcClient.EXPECT().GetConnectionHandle().Return(connectionHandle, nil)
		connectionHandle.EXPECT().Connection().Return(grpcClientConnection)
		projectionFactory.EXPECT().
			CreateClient(grpcClient, expectedProtoProjectionsClient).Return(projectionsClient)

		errorResult := errors.New("some error")

		projectionsClient.EXPECT().EnableProjection(ctx, connectionHandle, options).Return(errorResult)

		clientInstance := Client{
			grpcClient:              grpcClient,
			projectionClientFactory: projectionFactory,
		}

		err := clientInstance.EnableProjection(ctx, options)
		require.Error(t, err)
		require.EqualError(t, err, errorResult.Error())
	})

	t.Run("Projections client factory returned success", func(t *testing.T) {
		expectedProtoProjectionsClient := projectionsProto.NewProjectionsClient(grpcClientConnection)

		grpcClient.EXPECT().GetConnectionHandle().Return(connectionHandle, nil)
		connectionHandle.EXPECT().Connection().Return(grpcClientConnection)
		projectionFactory.EXPECT().
			CreateClient(grpcClient, expectedProtoProjectionsClient).Return(projectionsClient)
		projectionsClient.EXPECT().EnableProjection(ctx, connectionHandle, options).Return(nil)

		clientInstance := Client{
			grpcClient:              grpcClient,
			projectionClientFactory: projectionFactory,
		}

		err := clientInstance.EnableProjection(ctx, options)
		require.NoError(t, err)
	})
}

func TestClient_AbortProjection(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()
	grpcClient := connection.NewMockGrpcClient(ctrl)
	projectionFactory := projections.NewMockClientFactory(ctrl)
	connectionHandle := connection.NewMockConnectionHandle(ctrl)
	projectionsClient := projections.NewMockClient(ctrl)

	options := projections.AbortOptionsRequest{}
	options.SetName("some name")

	grpcClientConnection := &grpc.ClientConn{}

	t.Run("grpc client returned an error", func(t *testing.T) {
		errorResult := errors.New("some error")
		grpcClient.EXPECT().GetConnectionHandle().Return(nil, errorResult)

		clientInstance := Client{
			grpcClient: grpcClient,
		}

		err := clientInstance.AbortProjection(ctx, options)
		require.Error(t, err)
		require.EqualError(t, err, errorResult.Error())
	})

	t.Run("Projections client returned error", func(t *testing.T) {
		expectedProtoProjectionsClient := projectionsProto.NewProjectionsClient(grpcClientConnection)

		grpcClient.EXPECT().GetConnectionHandle().Return(connectionHandle, nil)
		connectionHandle.EXPECT().Connection().Return(grpcClientConnection)
		projectionFactory.EXPECT().
			CreateClient(grpcClient, expectedProtoProjectionsClient).Return(projectionsClient)

		errorResult := errors.New("some error")

		projectionsClient.EXPECT().AbortProjection(ctx, connectionHandle, options).Return(errorResult)

		clientInstance := Client{
			grpcClient:              grpcClient,
			projectionClientFactory: projectionFactory,
		}

		err := clientInstance.AbortProjection(ctx, options)
		require.Error(t, err)
		require.EqualError(t, err, errorResult.Error())
	})

	t.Run("Projections client returned success", func(t *testing.T) {
		expectedProtoProjectionsClient := projectionsProto.NewProjectionsClient(grpcClientConnection)

		grpcClient.EXPECT().GetConnectionHandle().Return(connectionHandle, nil)
		connectionHandle.EXPECT().Connection().Return(grpcClientConnection)
		projectionFactory.EXPECT().
			CreateClient(grpcClient, expectedProtoProjectionsClient).Return(projectionsClient)
		projectionsClient.EXPECT().AbortProjection(ctx, connectionHandle, options).Return(nil)

		clientInstance := Client{
			grpcClient:              grpcClient,
			projectionClientFactory: projectionFactory,
		}

		err := clientInstance.AbortProjection(ctx, options)
		require.NoError(t, err)
	})
}

func TestClient_DeleteProjection(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()
	grpcClient := connection.NewMockGrpcClient(ctrl)
	projectionFactory := projections.NewMockClientFactory(ctrl)
	connectionHandle := connection.NewMockConnectionHandle(ctrl)
	projectionsClient := projections.NewMockClient(ctrl)

	options := projections.DeleteOptionsRequest{}
	options.SetName("some name")

	grpcClientConnection := &grpc.ClientConn{}

	t.Run("grpc client returned an error", func(t *testing.T) {
		errorResult := errors.New("some error")
		grpcClient.EXPECT().GetConnectionHandle().Return(nil, errorResult)

		clientInstance := Client{
			grpcClient: grpcClient,
		}

		err := clientInstance.DeleteProjection(ctx, options)
		require.Error(t, err)
		require.EqualError(t, err, errorResult.Error())
	})

	t.Run("Projections client returned error", func(t *testing.T) {
		expectedProtoProjectionsClient := projectionsProto.NewProjectionsClient(grpcClientConnection)

		grpcClient.EXPECT().GetConnectionHandle().Return(connectionHandle, nil)
		connectionHandle.EXPECT().Connection().Return(grpcClientConnection)
		projectionFactory.EXPECT().
			CreateClient(grpcClient, expectedProtoProjectionsClient).Return(projectionsClient)

		errorResult := errors.New("some error")

		projectionsClient.EXPECT().DeleteProjection(ctx, connectionHandle, options).Return(errorResult)

		clientInstance := Client{
			grpcClient:              grpcClient,
			projectionClientFactory: projectionFactory,
		}

		err := clientInstance.DeleteProjection(ctx, options)
		require.Error(t, err)
		require.EqualError(t, err, errorResult.Error())
	})

	t.Run("Projections client returned success", func(t *testing.T) {
		expectedProtoProjectionsClient := projectionsProto.NewProjectionsClient(grpcClientConnection)

		grpcClient.EXPECT().GetConnectionHandle().Return(connectionHandle, nil)
		connectionHandle.EXPECT().Connection().Return(grpcClientConnection)
		projectionFactory.EXPECT().
			CreateClient(grpcClient, expectedProtoProjectionsClient).Return(projectionsClient)
		projectionsClient.EXPECT().DeleteProjection(ctx, connectionHandle, options).Return(nil)

		clientInstance := Client{
			grpcClient:              grpcClient,
			projectionClientFactory: projectionFactory,
		}

		err := clientInstance.DeleteProjection(ctx, options)
		require.NoError(t, err)
	})
}

func TestClient_GetProjectionResult(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()
	grpcClient := connection.NewMockGrpcClient(ctrl)
	projectionFactory := projections.NewMockClientFactory(ctrl)
	connectionHandle := connection.NewMockConnectionHandle(ctrl)
	projectionsClient := projections.NewMockClient(ctrl)

	options := projections.ResultOptionsRequest{}
	options.SetName("some name").SetPartition("some partition")

	grpcClientConnection := &grpc.ClientConn{}

	t.Run("grpc client returned an error", func(t *testing.T) {
		errorResult := errors.New("some error")
		grpcClient.EXPECT().GetConnectionHandle().Return(nil, errorResult)

		clientInstance := Client{
			grpcClient: grpcClient,
		}

		result, err := clientInstance.GetProjectionResult(ctx, options)
		require.Nil(t, result)
		require.Error(t, err)
		require.EqualError(t, err, errorResult.Error())
	})

	t.Run("Projections client returned error", func(t *testing.T) {
		expectedProtoProjectionsClient := projectionsProto.NewProjectionsClient(grpcClientConnection)

		grpcClient.EXPECT().GetConnectionHandle().Return(connectionHandle, nil)
		connectionHandle.EXPECT().Connection().Return(grpcClientConnection)
		projectionFactory.EXPECT().
			CreateClient(grpcClient, expectedProtoProjectionsClient).Return(projectionsClient)

		errorResult := errors.New("some error")

		projectionsClient.EXPECT().GetProjectionResult(ctx, connectionHandle, options).Return(nil, errorResult)

		clientInstance := Client{
			grpcClient:              grpcClient,
			projectionClientFactory: projectionFactory,
		}

		result, err := clientInstance.GetProjectionResult(ctx, options)
		require.Nil(t, result)
		require.Error(t, err)
		require.EqualError(t, err, errorResult.Error())
	})

	t.Run("Projections client factory returned success", func(t *testing.T) {
		expectedProtoProjectionsClient := projectionsProto.NewProjectionsClient(grpcClientConnection)

		grpcClient.EXPECT().GetConnectionHandle().Return(connectionHandle, nil)
		connectionHandle.EXPECT().Connection().Return(grpcClientConnection)
		projectionFactory.EXPECT().
			CreateClient(grpcClient, expectedProtoProjectionsClient).Return(projectionsClient)

		resultResponse := &projections.ResultResponseBool{}
		projectionsClient.EXPECT().GetProjectionResult(ctx, connectionHandle, options).Return(resultResponse, nil)

		clientInstance := Client{
			grpcClient:              grpcClient,
			projectionClientFactory: projectionFactory,
		}

		result, err := clientInstance.GetProjectionResult(ctx, options)
		require.Equal(t, resultResponse, result)
		require.NoError(t, err)
	})
}

func TestClient_GetProjectionState(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()
	grpcClient := connection.NewMockGrpcClient(ctrl)
	projectionFactory := projections.NewMockClientFactory(ctrl)
	connectionHandle := connection.NewMockConnectionHandle(ctrl)
	projectionsClient := projections.NewMockClient(ctrl)

	options := projections.StateOptionsRequest{}
	options.SetName("some name").SetPartition("some partition")

	grpcClientConnection := &grpc.ClientConn{}

	t.Run("grpc client returned an error", func(t *testing.T) {
		errorResult := errors.New("some error")
		grpcClient.EXPECT().GetConnectionHandle().Return(nil, errorResult)

		clientInstance := Client{
			grpcClient: grpcClient,
		}

		result, err := clientInstance.GetProjectionState(ctx, options)
		require.Nil(t, result)
		require.Error(t, err)
		require.EqualError(t, err, errorResult.Error())
	})

	t.Run("Projections client returned error", func(t *testing.T) {
		expectedProtoProjectionsClient := projectionsProto.NewProjectionsClient(grpcClientConnection)

		grpcClient.EXPECT().GetConnectionHandle().Return(connectionHandle, nil)
		connectionHandle.EXPECT().Connection().Return(grpcClientConnection)
		projectionFactory.EXPECT().
			CreateClient(grpcClient, expectedProtoProjectionsClient).Return(projectionsClient)

		errorResult := errors.New("some error")

		projectionsClient.EXPECT().GetProjectionState(ctx, connectionHandle, options).Return(nil, errorResult)

		clientInstance := Client{
			grpcClient:              grpcClient,
			projectionClientFactory: projectionFactory,
		}

		result, err := clientInstance.GetProjectionState(ctx, options)
		require.Nil(t, result)
		require.Error(t, err)
		require.EqualError(t, err, errorResult.Error())
	})

	t.Run("Projections client factory returned success", func(t *testing.T) {
		expectedProtoProjectionsClient := projectionsProto.NewProjectionsClient(grpcClientConnection)

		grpcClient.EXPECT().GetConnectionHandle().Return(connectionHandle, nil)
		connectionHandle.EXPECT().Connection().Return(grpcClientConnection)
		projectionFactory.EXPECT().
			CreateClient(grpcClient, expectedProtoProjectionsClient).Return(projectionsClient)

		resultResponse := &projections.StateResponseBool{}
		projectionsClient.EXPECT().GetProjectionState(ctx, connectionHandle, options).Return(resultResponse, nil)

		clientInstance := Client{
			grpcClient:              grpcClient,
			projectionClientFactory: projectionFactory,
		}

		result, err := clientInstance.GetProjectionState(ctx, options)
		require.Equal(t, resultResponse, result)
		require.NoError(t, err)
	})
}

func TestClient_GetProjectionStatistics(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()
	grpcClient := connection.NewMockGrpcClient(ctrl)
	projectionFactory := projections.NewMockClientFactory(ctrl)
	connectionHandle := connection.NewMockConnectionHandle(ctrl)
	projectionsClient := projections.NewMockClient(ctrl)

	options := projections.StatisticsOptionsRequest{}
	options.SetMode(projections.StatisticsOptionsRequestModeOneTime{})

	grpcClientConnection := &grpc.ClientConn{}

	t.Run("grpc client returned an error", func(t *testing.T) {
		errorResult := errors.New("some error")
		grpcClient.EXPECT().GetConnectionHandle().Return(nil, errorResult)

		clientInstance := Client{
			grpcClient: grpcClient,
		}

		result, err := clientInstance.GetProjectionStatistics(ctx, options)
		require.Nil(t, result)
		require.Error(t, err)
		require.EqualError(t, err, errorResult.Error())
	})

	t.Run("Projections client returned error", func(t *testing.T) {
		expectedProtoProjectionsClient := projectionsProto.NewProjectionsClient(grpcClientConnection)

		grpcClient.EXPECT().GetConnectionHandle().Return(connectionHandle, nil)
		connectionHandle.EXPECT().Connection().Return(grpcClientConnection)
		projectionFactory.EXPECT().
			CreateClient(grpcClient, expectedProtoProjectionsClient).Return(projectionsClient)

		errorResult := errors.New("some error")

		projectionsClient.EXPECT().GetProjectionStatistics(ctx, connectionHandle, options).Return(nil, errorResult)

		clientInstance := Client{
			grpcClient:              grpcClient,
			projectionClientFactory: projectionFactory,
		}

		result, err := clientInstance.GetProjectionStatistics(ctx, options)
		require.Nil(t, result)
		require.Error(t, err)
		require.EqualError(t, err, errorResult.Error())
	})

	t.Run("Projections client factory returned success", func(t *testing.T) {
		statisticsClient := projections.NewMockStatisticsClientSync(ctrl)

		expectedProtoProjectionsClient := projectionsProto.NewProjectionsClient(grpcClientConnection)

		grpcClient.EXPECT().GetConnectionHandle().Return(connectionHandle, nil)
		connectionHandle.EXPECT().Connection().Return(grpcClientConnection)
		projectionFactory.EXPECT().
			CreateClient(grpcClient, expectedProtoProjectionsClient).Return(projectionsClient)

		projectionsClient.EXPECT().GetProjectionStatistics(ctx, connectionHandle, options).Return(statisticsClient, nil)

		clientInstance := Client{
			grpcClient:              grpcClient,
			projectionClientFactory: projectionFactory,
		}

		result, err := clientInstance.GetProjectionStatistics(ctx, options)
		require.Equal(t, statisticsClient, result)
		require.NoError(t, err)
	})
}

func TestClient_RestartProjectionsSubsystem(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()
	grpcClient := connection.NewMockGrpcClient(ctrl)
	projectionFactory := projections.NewMockClientFactory(ctrl)
	connectionHandle := connection.NewMockConnectionHandle(ctrl)
	projectionsClient := projections.NewMockClient(ctrl)

	grpcClientConnection := &grpc.ClientConn{}

	t.Run("grpc client returned an error", func(t *testing.T) {
		errorResult := errors.New("some error")
		grpcClient.EXPECT().GetConnectionHandle().Return(nil, errorResult)

		clientInstance := Client{
			grpcClient: grpcClient,
		}

		err := clientInstance.RestartProjectionsSubsystem(ctx)
		require.Error(t, err)
		require.EqualError(t, err, errorResult.Error())
	})

	t.Run("Projections client returned error", func(t *testing.T) {
		expectedProtoProjectionsClient := projectionsProto.NewProjectionsClient(grpcClientConnection)

		grpcClient.EXPECT().GetConnectionHandle().Return(connectionHandle, nil)
		connectionHandle.EXPECT().Connection().Return(grpcClientConnection)
		projectionFactory.EXPECT().
			CreateClient(grpcClient, expectedProtoProjectionsClient).Return(projectionsClient)

		errorResult := errors.New("some error")

		projectionsClient.EXPECT().RestartProjectionsSubsystem(ctx, connectionHandle).Return(errorResult)

		clientInstance := Client{
			grpcClient:              grpcClient,
			projectionClientFactory: projectionFactory,
		}

		err := clientInstance.RestartProjectionsSubsystem(ctx)
		require.Error(t, err)
		require.EqualError(t, err, errorResult.Error())
	})

	t.Run("Projections client returned success", func(t *testing.T) {
		expectedProtoProjectionsClient := projectionsProto.NewProjectionsClient(grpcClientConnection)

		grpcClient.EXPECT().GetConnectionHandle().Return(connectionHandle, nil)
		connectionHandle.EXPECT().Connection().Return(grpcClientConnection)
		projectionFactory.EXPECT().
			CreateClient(grpcClient, expectedProtoProjectionsClient).Return(projectionsClient)
		projectionsClient.EXPECT().RestartProjectionsSubsystem(ctx, connectionHandle).Return(nil)

		clientInstance := Client{
			grpcClient:              grpcClient,
			projectionClientFactory: projectionFactory,
		}

		err := clientInstance.RestartProjectionsSubsystem(ctx)
		require.NoError(t, err)
	})
}

func TestClient_ListAllProjections(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()
	grpcClient := connection.NewMockGrpcClient(ctrl)
	projectionFactory := projections.NewMockClientFactory(ctrl)
	connectionHandle := connection.NewMockConnectionHandle(ctrl)
	projectionsClient := projections.NewMockClient(ctrl)

	grpcClientConnection := &grpc.ClientConn{}

	t.Run("grpc client returned an error", func(t *testing.T) {
		errorResult := errors.New("some error")
		grpcClient.EXPECT().GetConnectionHandle().Return(nil, errorResult)

		clientInstance := Client{
			grpcClient: grpcClient,
		}

		result, err := clientInstance.ListAllProjections(ctx)
		require.Nil(t, result)
		require.Error(t, err)
		require.EqualError(t, err, errorResult.Error())
	})

	t.Run("Projections client returned error", func(t *testing.T) {
		expectedProtoProjectionsClient := projectionsProto.NewProjectionsClient(grpcClientConnection)

		grpcClient.EXPECT().GetConnectionHandle().Return(connectionHandle, nil)
		connectionHandle.EXPECT().Connection().Return(grpcClientConnection)
		projectionFactory.EXPECT().
			CreateClient(grpcClient, expectedProtoProjectionsClient).Return(projectionsClient)

		errorResult := errors.New("some error")

		projectionsClient.EXPECT().ListAllProjections(ctx, connectionHandle).Return(nil, errorResult)

		clientInstance := Client{
			grpcClient:              grpcClient,
			projectionClientFactory: projectionFactory,
		}

		result, err := clientInstance.ListAllProjections(ctx)
		require.Nil(t, result)
		require.Error(t, err)
		require.EqualError(t, err, errorResult.Error())
	})

	t.Run("Projections client factory returned success", func(t *testing.T) {
		expectedProtoProjectionsClient := projectionsProto.NewProjectionsClient(grpcClientConnection)

		grpcClient.EXPECT().GetConnectionHandle().Return(connectionHandle, nil)
		connectionHandle.EXPECT().Connection().Return(grpcClientConnection)
		projectionFactory.EXPECT().
			CreateClient(grpcClient, expectedProtoProjectionsClient).Return(projectionsClient)

		resultResponse := []projections.StatisticsClientResponse{
			{
				Name: "some name",
			},
			{
				Name: "some name 2",
			},
		}
		projectionsClient.EXPECT().ListAllProjections(ctx, connectionHandle).Return(resultResponse, nil)

		clientInstance := Client{
			grpcClient:              grpcClient,
			projectionClientFactory: projectionFactory,
		}

		result, err := clientInstance.ListAllProjections(ctx)
		require.Equal(t, resultResponse, result)
		require.NoError(t, err)
	})
}

func TestClient_ListContinuousProjections(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()
	grpcClient := connection.NewMockGrpcClient(ctrl)
	projectionFactory := projections.NewMockClientFactory(ctrl)
	connectionHandle := connection.NewMockConnectionHandle(ctrl)
	projectionsClient := projections.NewMockClient(ctrl)

	grpcClientConnection := &grpc.ClientConn{}

	t.Run("grpc client returned an error", func(t *testing.T) {
		errorResult := errors.New("some error")
		grpcClient.EXPECT().GetConnectionHandle().Return(nil, errorResult)

		clientInstance := Client{
			grpcClient: grpcClient,
		}

		result, err := clientInstance.ListContinuousProjections(ctx)
		require.Nil(t, result)
		require.Error(t, err)
		require.EqualError(t, err, errorResult.Error())
	})

	t.Run("Projections client returned error", func(t *testing.T) {
		expectedProtoProjectionsClient := projectionsProto.NewProjectionsClient(grpcClientConnection)

		grpcClient.EXPECT().GetConnectionHandle().Return(connectionHandle, nil)
		connectionHandle.EXPECT().Connection().Return(grpcClientConnection)
		projectionFactory.EXPECT().
			CreateClient(grpcClient, expectedProtoProjectionsClient).Return(projectionsClient)

		errorResult := errors.New("some error")

		projectionsClient.EXPECT().ListContinuousProjections(ctx, connectionHandle).Return(nil, errorResult)

		clientInstance := Client{
			grpcClient:              grpcClient,
			projectionClientFactory: projectionFactory,
		}

		result, err := clientInstance.ListContinuousProjections(ctx)
		require.Nil(t, result)
		require.Error(t, err)
		require.EqualError(t, err, errorResult.Error())
	})

	t.Run("Projections client factory returned success", func(t *testing.T) {
		expectedProtoProjectionsClient := projectionsProto.NewProjectionsClient(grpcClientConnection)

		grpcClient.EXPECT().GetConnectionHandle().Return(connectionHandle, nil)
		connectionHandle.EXPECT().Connection().Return(grpcClientConnection)
		projectionFactory.EXPECT().
			CreateClient(grpcClient, expectedProtoProjectionsClient).Return(projectionsClient)

		resultResponse := []projections.StatisticsClientResponse{
			{
				Name: "some name",
			},
			{
				Name: "some name 2",
			},
		}
		projectionsClient.EXPECT().ListContinuousProjections(ctx, connectionHandle).Return(resultResponse, nil)

		clientInstance := Client{
			grpcClient:              grpcClient,
			projectionClientFactory: projectionFactory,
		}

		result, err := clientInstance.ListContinuousProjections(ctx)
		require.Equal(t, resultResponse, result)
		require.NoError(t, err)
	})
}

func TestClient_ListOneTimeProjections(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()
	grpcClient := connection.NewMockGrpcClient(ctrl)
	projectionFactory := projections.NewMockClientFactory(ctrl)
	connectionHandle := connection.NewMockConnectionHandle(ctrl)
	projectionsClient := projections.NewMockClient(ctrl)

	grpcClientConnection := &grpc.ClientConn{}

	t.Run("grpc client returned an error", func(t *testing.T) {
		errorResult := errors.New("some error")
		grpcClient.EXPECT().GetConnectionHandle().Return(nil, errorResult)

		clientInstance := Client{
			grpcClient: grpcClient,
		}

		result, err := clientInstance.ListOneTimeProjections(ctx)
		require.Nil(t, result)
		require.Error(t, err)
		require.EqualError(t, err, errorResult.Error())
	})

	t.Run("Projections client returned error", func(t *testing.T) {
		expectedProtoProjectionsClient := projectionsProto.NewProjectionsClient(grpcClientConnection)

		grpcClient.EXPECT().GetConnectionHandle().Return(connectionHandle, nil)
		connectionHandle.EXPECT().Connection().Return(grpcClientConnection)
		projectionFactory.EXPECT().
			CreateClient(grpcClient, expectedProtoProjectionsClient).Return(projectionsClient)

		errorResult := errors.New("some error")

		projectionsClient.EXPECT().ListOneTimeProjections(ctx, connectionHandle).Return(nil, errorResult)

		clientInstance := Client{
			grpcClient:              grpcClient,
			projectionClientFactory: projectionFactory,
		}

		result, err := clientInstance.ListOneTimeProjections(ctx)
		require.Nil(t, result)
		require.Error(t, err)
		require.EqualError(t, err, errorResult.Error())
	})

	t.Run("Projections client factory returned success", func(t *testing.T) {
		expectedProtoProjectionsClient := projectionsProto.NewProjectionsClient(grpcClientConnection)

		grpcClient.EXPECT().GetConnectionHandle().Return(connectionHandle, nil)
		connectionHandle.EXPECT().Connection().Return(grpcClientConnection)
		projectionFactory.EXPECT().
			CreateClient(grpcClient, expectedProtoProjectionsClient).Return(projectionsClient)

		resultResponse := []projections.StatisticsClientResponse{
			{
				Name: "some name",
			},
			{
				Name: "some name 2",
			},
		}
		projectionsClient.EXPECT().ListOneTimeProjections(ctx, connectionHandle).Return(resultResponse, nil)

		clientInstance := Client{
			grpcClient:              grpcClient,
			projectionClientFactory: projectionFactory,
		}

		result, err := clientInstance.ListOneTimeProjections(ctx)
		require.Equal(t, resultResponse, result)
		require.NoError(t, err)
	})
}
