package projections

import (
	"context"
	"errors"
	"testing"

	"github.com/EventStore/EventStore-Client-Go/protos/persistent"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/EventStore/EventStore-Client-Go/connection"

	"github.com/EventStore/EventStore-Client-Go/protos/shared"

	"google.golang.org/protobuf/types/known/structpb"

	"github.com/stretchr/testify/require"

	"github.com/EventStore/EventStore-Client-Go/protos/projections"
	"github.com/golang/mock/gomock"
)

func TestClientImpl_CreateProjection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)

	ctx := context.Background()

	options := CreateOptionsRequest{}
	options.SetQuery("some query")
	options.SetMode(CreateConfigModeContinuousOption{
		Name:                "some mode",
		TrackEmittedStreams: true,
	})

	grpcOptions := options.Build()

	t.Run("Success", func(t *testing.T) {
		var headers, trailers metadata.MD
		grpcProjectionsClientMock.EXPECT().Create(ctx, grpcOptions,
			grpc.Header(&headers), grpc.Trailer(&trailers)).Times(1).Return(nil, nil)

		client := ClientImpl{
			projectionsClient: grpcProjectionsClientMock,
		}

		err := client.CreateProjection(ctx, handle, options)
		require.NoError(t, err)
	})

	t.Run("Error returned from grpc", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)

		errorResult := errors.New("some error")
		expectedHeader := metadata.MD{
			"header_key": []string{"header_value"},
		}

		expectedTrailer := metadata.MD{
			"trailer_key": []string{"trailer_value"},
		}

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcProjectionsClientMock.EXPECT().Create(ctx, grpcOptions,
				grpc.Header(&headers), grpc.Trailer(&trailers)).
				DoAndReturn(func(
					_ctx context.Context,
					_protoRequest *projections.CreateReq,
					options ...grpc.CallOption) (persistent.PersistentSubscriptions_ReadClient, error) {

					*options[0].(grpc.HeaderCallOption).HeaderAddr = metadata.MD{
						"header_key": []string{"header_value"},
					}

					*options[1].(grpc.TrailerCallOption).TrailerAddr = metadata.MD{
						"trailer_key": []string{"trailer_value"},
					}
					return nil, errorResult
				}),
			grpcClient.EXPECT().HandleError(handle, expectedHeader, expectedTrailer, errorResult),
		)

		client := ClientImpl{
			projectionsClient: grpcProjectionsClientMock,
			grpcClient:        grpcClient,
		}

		err := client.CreateProjection(ctx, handle, options)
		require.EqualError(t, err, FailedToCreateProjectionErr)
	})
}

func TestClientImpl_UpdateProjection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)
	ctx := context.Background()

	options := UpdateOptionsRequest{}
	options.SetName("some name").
		SetQuery("some query").
		SetEmitOption(UpdateOptionsEmitOptionEnabled{
			EmitEnabled: true,
		})

	grpcOptions := options.Build()

	t.Run("Success", func(t *testing.T) {
		var headers, trailers metadata.MD
		grpcProjectionsClientMock.EXPECT().Update(ctx, grpcOptions,
			grpc.Header(&headers), grpc.Trailer(&trailers)).Times(1).Return(nil, nil)

		client := ClientImpl{
			projectionsClient: grpcProjectionsClientMock,
		}

		err := client.UpdateProjection(ctx, handle, options)
		require.NoError(t, err)
	})

	t.Run("Error returned from grpc", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)

		errorResult := errors.New("some error")
		expectedHeader := metadata.MD{
			"header_key": []string{"header_value"},
		}

		expectedTrailer := metadata.MD{
			"trailer_key": []string{"trailer_value"},
		}

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcProjectionsClientMock.EXPECT().Update(ctx, grpcOptions,
				grpc.Header(&headers), grpc.Trailer(&trailers)).
				DoAndReturn(func(
					_ctx context.Context,
					_protoRequest *projections.UpdateReq,
					options ...grpc.CallOption) (*persistent.UpdateResp, error) {

					*options[0].(grpc.HeaderCallOption).HeaderAddr = metadata.MD{
						"header_key": []string{"header_value"},
					}

					*options[1].(grpc.TrailerCallOption).TrailerAddr = metadata.MD{
						"trailer_key": []string{"trailer_value"},
					}
					return nil, errorResult
				}),
			grpcClient.EXPECT().HandleError(handle, expectedHeader, expectedTrailer, errorResult),
		)

		client := ClientImpl{
			projectionsClient: grpcProjectionsClientMock,
			grpcClient:        grpcClient,
		}

		err := client.UpdateProjection(ctx, handle, options)
		require.EqualError(t, err, FailedToUpdateProjectionErr)
	})
}

func TestClientImpl_DeleteProjection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)
	ctx := context.Background()

	options := DeleteOptionsRequest{}
	options.SetName("some name")

	grpcOptions := options.Build()

	t.Run("Success", func(t *testing.T) {
		var headers, trailers metadata.MD
		grpcProjectionsClientMock.EXPECT().Delete(ctx, grpcOptions,
			grpc.Header(&headers), grpc.Trailer(&trailers)).Times(1).Return(nil, nil)

		client := ClientImpl{
			projectionsClient: grpcProjectionsClientMock,
		}

		err := client.DeleteProjection(ctx, handle, options)
		require.NoError(t, err)
	})

	t.Run("Error returned from grpc", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)

		errorResult := errors.New("some error")
		expectedHeader := metadata.MD{
			"header_key": []string{"header_value"},
		}

		expectedTrailer := metadata.MD{
			"trailer_key": []string{"trailer_value"},
		}

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcProjectionsClientMock.EXPECT().Delete(ctx, grpcOptions,
				grpc.Header(&headers), grpc.Trailer(&trailers)).
				DoAndReturn(func(
					_ctx context.Context,
					_protoRequest *projections.DeleteReq,
					options ...grpc.CallOption) (*persistent.DeleteResp, error) {

					*options[0].(grpc.HeaderCallOption).HeaderAddr = metadata.MD{
						"header_key": []string{"header_value"},
					}

					*options[1].(grpc.TrailerCallOption).TrailerAddr = metadata.MD{
						"trailer_key": []string{"trailer_value"},
					}
					return nil, errorResult
				}),
			grpcClient.EXPECT().HandleError(handle, expectedHeader, expectedTrailer, errorResult),
		)

		client := ClientImpl{
			projectionsClient: grpcProjectionsClientMock,
			grpcClient:        grpcClient,
		}

		err := client.DeleteProjection(ctx, handle, options)
		require.EqualError(t, err, FailedToDeleteProjectionErr)
	})
}

func TestClientImpl_ProjectionStatistics(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)
	ctx := context.Background()

	options := StatisticsOptionsRequest{}
	options.SetMode(StatisticsOptionsRequestModeAll{})

	grpcOptions := options.Build()

	t.Run("Success", func(t *testing.T) {
		var headers, trailers metadata.MD
		grpcProjectionsClientMock.EXPECT().Statistics(ctx, grpcOptions,
			grpc.Header(&headers), grpc.Trailer(&trailers)).Times(1).Return(nil, nil)

		client := ClientImpl{
			projectionsClient: grpcProjectionsClientMock,
		}

		statisticsClient, err := client.ProjectionStatistics(ctx, handle, options)
		require.NotNil(t, statisticsClient)
		require.NoError(t, err)
	})

	t.Run("Error returned from grpc", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)

		errorResult := errors.New("some error")
		expectedHeader := metadata.MD{
			"header_key": []string{"header_value"},
		}

		expectedTrailer := metadata.MD{
			"trailer_key": []string{"trailer_value"},
		}

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcProjectionsClientMock.EXPECT().Statistics(ctx, grpcOptions,
				grpc.Header(&headers), grpc.Trailer(&trailers)).
				DoAndReturn(func(
					_ctx context.Context,
					_protoRequest *projections.StatisticsReq,
					options ...grpc.CallOption) (projections.Projections_StatisticsClient, error) {

					*options[0].(grpc.HeaderCallOption).HeaderAddr = metadata.MD{
						"header_key": []string{"header_value"},
					}

					*options[1].(grpc.TrailerCallOption).TrailerAddr = metadata.MD{
						"trailer_key": []string{"trailer_value"},
					}
					return nil, errorResult
				}),
			grpcClient.EXPECT().HandleError(handle, expectedHeader, expectedTrailer, errorResult),
		)

		client := ClientImpl{
			projectionsClient: grpcProjectionsClientMock,
			grpcClient:        grpcClient,
		}

		statisticsClient, err := client.ProjectionStatistics(ctx, handle, options)
		require.Nil(t, statisticsClient)
		require.EqualError(t, err, FailedToFetchProjectionStatisticsErr)
	})
}

func TestClientImpl_DisableProjection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)
	ctx := context.Background()

	options := DisableOptionsRequest{}
	grpcOptions := options.Build()

	t.Run("Success", func(t *testing.T) {
		var headers, trailers metadata.MD
		grpcProjectionsClientMock.EXPECT().Disable(ctx, grpcOptions,
			grpc.Header(&headers), grpc.Trailer(&trailers)).Times(1).Return(nil, nil)

		client := ClientImpl{
			projectionsClient: grpcProjectionsClientMock,
		}

		err := client.DisableProjection(ctx, handle, options)
		require.NoError(t, err)
	})

	t.Run("Error returned from grpc", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)

		errorResult := errors.New("some error")
		expectedHeader := metadata.MD{
			"header_key": []string{"header_value"},
		}

		expectedTrailer := metadata.MD{
			"trailer_key": []string{"trailer_value"},
		}

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcProjectionsClientMock.EXPECT().Disable(ctx, grpcOptions,
				grpc.Header(&headers), grpc.Trailer(&trailers)).
				DoAndReturn(func(
					_ctx context.Context,
					_protoRequest *projections.DisableReq,
					options ...grpc.CallOption) (*projections.DisableResp, error) {

					*options[0].(grpc.HeaderCallOption).HeaderAddr = metadata.MD{
						"header_key": []string{"header_value"},
					}

					*options[1].(grpc.TrailerCallOption).TrailerAddr = metadata.MD{
						"trailer_key": []string{"trailer_value"},
					}
					return nil, errorResult
				}),
			grpcClient.EXPECT().HandleError(handle, expectedHeader, expectedTrailer, errorResult),
		)

		client := ClientImpl{
			projectionsClient: grpcProjectionsClientMock,
			grpcClient:        grpcClient,
		}

		err := client.DisableProjection(ctx, handle, options)
		require.EqualError(t, err, FailedToDisableProjectionErr)
	})
}

func TestClientImpl_AbortProjection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)
	ctx := context.Background()

	options := AbortOptionsRequest{}
	grpcOptions := options.Build()

	t.Run("Success", func(t *testing.T) {
		var headers, trailers metadata.MD
		grpcProjectionsClientMock.EXPECT().Disable(ctx, grpcOptions,
			grpc.Header(&headers), grpc.Trailer(&trailers)).Times(1).Return(nil, nil)

		client := ClientImpl{
			projectionsClient: grpcProjectionsClientMock,
		}

		err := client.AbortProjection(ctx, handle, options)
		require.NoError(t, err)
	})

	t.Run("Error returned from grpc", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)

		errorResult := errors.New("some error")
		expectedHeader := metadata.MD{
			"header_key": []string{"header_value"},
		}

		expectedTrailer := metadata.MD{
			"trailer_key": []string{"trailer_value"},
		}

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcProjectionsClientMock.EXPECT().Disable(ctx, grpcOptions,
				grpc.Header(&headers), grpc.Trailer(&trailers)).
				DoAndReturn(func(
					_ctx context.Context,
					_protoRequest *projections.DisableReq,
					options ...grpc.CallOption) (*projections.DisableResp, error) {

					*options[0].(grpc.HeaderCallOption).HeaderAddr = metadata.MD{
						"header_key": []string{"header_value"},
					}

					*options[1].(grpc.TrailerCallOption).TrailerAddr = metadata.MD{
						"trailer_key": []string{"trailer_value"},
					}
					return nil, errorResult
				}),
			grpcClient.EXPECT().HandleError(handle, expectedHeader, expectedTrailer, errorResult),
		)

		client := ClientImpl{
			projectionsClient: grpcProjectionsClientMock,
			grpcClient:        grpcClient,
		}

		err := client.AbortProjection(ctx, handle, options)
		require.EqualError(t, err, FailedToAbortProjectionErr)
	})
}

func TestClientImpl_EnableProjection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)
	ctx := context.Background()

	options := EnableOptionsRequest{}
	options.SetName("some name")
	grpcOptions := options.Build()

	t.Run("Success", func(t *testing.T) {
		var headers, trailers metadata.MD
		grpcProjectionsClientMock.EXPECT().Enable(ctx, grpcOptions,
			grpc.Header(&headers), grpc.Trailer(&trailers)).Times(1).Return(nil, nil)

		client := ClientImpl{
			projectionsClient: grpcProjectionsClientMock,
		}

		err := client.EnableProjection(ctx, handle, options)
		require.NoError(t, err)
	})

	t.Run("Error returned from grpc", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)

		errorResult := errors.New("some error")
		expectedHeader := metadata.MD{
			"header_key": []string{"header_value"},
		}

		expectedTrailer := metadata.MD{
			"trailer_key": []string{"trailer_value"},
		}

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcProjectionsClientMock.EXPECT().Enable(ctx, grpcOptions,
				grpc.Header(&headers), grpc.Trailer(&trailers)).
				DoAndReturn(func(
					_ctx context.Context,
					_protoRequest *projections.EnableReq,
					options ...grpc.CallOption) (*projections.EnableResp, error) {

					*options[0].(grpc.HeaderCallOption).HeaderAddr = metadata.MD{
						"header_key": []string{"header_value"},
					}

					*options[1].(grpc.TrailerCallOption).TrailerAddr = metadata.MD{
						"trailer_key": []string{"trailer_value"},
					}
					return nil, errorResult
				}),
			grpcClient.EXPECT().HandleError(handle, expectedHeader, expectedTrailer, errorResult),
		)

		client := ClientImpl{
			projectionsClient: grpcProjectionsClientMock,
			grpcClient:        grpcClient,
		}
		err := client.EnableProjection(ctx, handle, options)
		require.EqualError(t, err, FailedToEnableProjectionErr)
	})
}

func TestClientImpl_ResetProjection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)
	ctx := context.Background()

	options := ResetOptionsRequest{}
	options.SetName("some name").SetWriteCheckpoint(true)
	grpcOptions := options.Build()

	t.Run("Success", func(t *testing.T) {
		var headers, trailers metadata.MD
		grpcProjectionsClientMock.EXPECT().Reset(ctx, grpcOptions,
			grpc.Header(&headers), grpc.Trailer(&trailers)).Times(1).Return(nil, nil)

		client := ClientImpl{
			projectionsClient: grpcProjectionsClientMock,
		}

		err := client.ResetProjection(ctx, handle, options)
		require.NoError(t, err)
	})

	t.Run("Error returned from grpc", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)

		errorResult := errors.New("some error")
		expectedHeader := metadata.MD{
			"header_key": []string{"header_value"},
		}

		expectedTrailer := metadata.MD{
			"trailer_key": []string{"trailer_value"},
		}

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcProjectionsClientMock.EXPECT().Reset(ctx, grpcOptions,
				grpc.Header(&headers), grpc.Trailer(&trailers)).
				DoAndReturn(func(
					_ctx context.Context,
					_protoRequest *projections.ResetReq,
					options ...grpc.CallOption) (*projections.ResetResp, error) {

					*options[0].(grpc.HeaderCallOption).HeaderAddr = metadata.MD{
						"header_key": []string{"header_value"},
					}

					*options[1].(grpc.TrailerCallOption).TrailerAddr = metadata.MD{
						"trailer_key": []string{"trailer_value"},
					}
					return nil, errorResult
				}),
			grpcClient.EXPECT().HandleError(handle, expectedHeader, expectedTrailer, errorResult),
		)

		client := ClientImpl{
			projectionsClient: grpcProjectionsClientMock,
			grpcClient:        grpcClient,
		}

		err := client.ResetProjection(ctx, handle, options)
		require.EqualError(t, err, FailedToResetProjectionErr)
	})
}

func TestClientImpl_ProjectionState(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)
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
		var headers, trailers metadata.MD
		grpcProjectionsClientMock.EXPECT().State(ctx, grpcOptions,
			grpc.Header(&headers), grpc.Trailer(&trailers)).Times(1).Return(response, nil)

		client := ClientImpl{
			projectionsClient: grpcProjectionsClientMock,
		}

		expectedStateResponse := newStateResponse(response)

		state, err := client.GetProjectionState(ctx, handle, options)
		require.Equal(t, expectedStateResponse, state)
		require.NoError(t, err)
	})

	t.Run("Error returned from grpc", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)

		errorResult := errors.New("some error")
		expectedHeader := metadata.MD{
			"header_key": []string{"header_value"},
		}

		expectedTrailer := metadata.MD{
			"trailer_key": []string{"trailer_value"},
		}

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcProjectionsClientMock.EXPECT().State(ctx, grpcOptions,
				grpc.Header(&headers), grpc.Trailer(&trailers)).
				DoAndReturn(func(
					_ctx context.Context,
					_protoRequest *projections.StateReq,
					options ...grpc.CallOption) (*projections.StateResp, error) {

					*options[0].(grpc.HeaderCallOption).HeaderAddr = metadata.MD{
						"header_key": []string{"header_value"},
					}

					*options[1].(grpc.TrailerCallOption).TrailerAddr = metadata.MD{
						"trailer_key": []string{"trailer_value"},
					}
					return nil, errorResult
				}),
			grpcClient.EXPECT().HandleError(handle, expectedHeader, expectedTrailer, errorResult),
		)

		client := ClientImpl{
			projectionsClient: grpcProjectionsClientMock,
			grpcClient:        grpcClient,
		}

		state, err := client.GetProjectionState(ctx, handle, options)
		require.Nil(t, state)
		require.EqualError(t, err, FailedToGetProjectionStateErr)
	})
}

func TestClientImpl_ProjectionResult(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)
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
		var headers, trailers metadata.MD
		grpcProjectionsClientMock.EXPECT().Result(ctx, grpcOptions,
			grpc.Header(&headers), grpc.Trailer(&trailers)).Times(1).Return(response, nil)

		client := ClientImpl{
			projectionsClient: grpcProjectionsClientMock,
		}

		expectedStateResponse := newResultResponse(response)

		state, err := client.GetProjectionResult(ctx, handle, options)
		require.Equal(t, expectedStateResponse, state)
		require.NoError(t, err)
	})

	t.Run("Error returned from grpc", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)

		errorResult := errors.New("some error")
		expectedHeader := metadata.MD{
			"header_key": []string{"header_value"},
		}

		expectedTrailer := metadata.MD{
			"trailer_key": []string{"trailer_value"},
		}

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcProjectionsClientMock.EXPECT().Result(ctx, grpcOptions,
				grpc.Header(&headers), grpc.Trailer(&trailers)).
				DoAndReturn(func(
					_ctx context.Context,
					_protoRequest *projections.ResultReq,
					options ...grpc.CallOption) (*projections.ResultResp, error) {

					*options[0].(grpc.HeaderCallOption).HeaderAddr = metadata.MD{
						"header_key": []string{"header_value"},
					}

					*options[1].(grpc.TrailerCallOption).TrailerAddr = metadata.MD{
						"trailer_key": []string{"trailer_value"},
					}
					return nil, errorResult
				}),
			grpcClient.EXPECT().HandleError(handle, expectedHeader, expectedTrailer, errorResult),
		)

		client := ClientImpl{
			projectionsClient: grpcProjectionsClientMock,
			grpcClient:        grpcClient,
		}

		state, err := client.GetProjectionResult(ctx, handle, options)
		require.Nil(t, state)
		require.EqualError(t, err, FailedToGetProjectionResultErr)
	})
}

func TestClientImpl_RestartProjectionsSubsystem(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)
	ctx := context.Background()

	t.Run("Success with RestartSubsystem return nil", func(t *testing.T) {
		var headers, trailers metadata.MD
		grpcProjectionsClientMock.EXPECT().RestartSubsystem(ctx, &shared.Empty{},
			grpc.Header(&headers), grpc.Trailer(&trailers)).Times(1).Return(nil, nil)

		client := ClientImpl{
			projectionsClient: grpcProjectionsClientMock,
		}

		err := client.RestartProjectionsSubsystem(ctx, handle)
		require.NoError(t, err)
	})

	t.Run("Success with RestartSubsystem return &shared.Empty{}", func(t *testing.T) {
		var headers, trailers metadata.MD
		grpcProjectionsClientMock.EXPECT().RestartSubsystem(ctx, &shared.Empty{},
			grpc.Header(&headers), grpc.Trailer(&trailers)).Times(1).Return(&shared.Empty{}, nil)

		client := ClientImpl{
			projectionsClient: grpcProjectionsClientMock,
		}

		err := client.RestartProjectionsSubsystem(ctx, handle)
		require.NoError(t, err)
	})

	t.Run("Error returned from grpc", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)

		errorResult := errors.New("some error")
		expectedHeader := metadata.MD{
			"header_key": []string{"header_value"},
		}

		expectedTrailer := metadata.MD{
			"trailer_key": []string{"trailer_value"},
		}

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcProjectionsClientMock.EXPECT().RestartSubsystem(ctx, &shared.Empty{},
				grpc.Header(&headers), grpc.Trailer(&trailers)).
				DoAndReturn(func(
					_ctx context.Context,
					_protoRequest *shared.Empty,
					options ...grpc.CallOption) (*shared.Empty, error) {

					*options[0].(grpc.HeaderCallOption).HeaderAddr = metadata.MD{
						"header_key": []string{"header_value"},
					}

					*options[1].(grpc.TrailerCallOption).TrailerAddr = metadata.MD{
						"trailer_key": []string{"trailer_value"},
					}
					return nil, errorResult
				}),
			grpcClient.EXPECT().HandleError(handle, expectedHeader, expectedTrailer, errorResult),
		)

		client := ClientImpl{
			projectionsClient: grpcProjectionsClientMock,
			grpcClient:        grpcClient,
		}

		err := client.RestartProjectionsSubsystem(ctx, handle)
		require.EqualError(t, err, FailedToRestartProjectionsSubsystemErr)
	})
}
