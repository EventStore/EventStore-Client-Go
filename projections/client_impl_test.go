package projections

import (
	"context"
	"io"
	"testing"

	"github.com/pivonroll/EventStore-Client-Go/errors"
	"github.com/pivonroll/EventStore-Client-Go/protos/shared"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/pivonroll/EventStore-Client-Go/protos/persistent"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/pivonroll/EventStore-Client-Go/connection"

	"github.com/stretchr/testify/require"

	"github.com/golang/mock/gomock"
	"github.com/pivonroll/EventStore-Client-Go/protos/projections"
)

func TestClientImpl_CreateProjection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()

	grpcClientConn := &grpc.ClientConn{}
	options := CreateOptionsRequest{}
	options.SetQuery("some query")
	options.SetMode(CreateConfigModeContinuousOption{
		Name:                "some mode",
		TrackEmittedStreams: true,
	})

	grpcOptions := options.Build()

	t.Run("Success", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		handle := connection.NewMockConnectionHandle(ctrl)
		grpcProjectionsClientFactoryInstance := NewMockgrpcProjectionsClientFactory(ctrl)
		grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)

		var headers, trailers metadata.MD
		gomock.InOrder(
			grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
			handle.EXPECT().Connection().Return(grpcClientConn),
			grpcProjectionsClientFactoryInstance.EXPECT().Create(grpcClientConn).
				Return(grpcProjectionsClientMock),
			grpcProjectionsClientMock.EXPECT().Create(ctx, grpcOptions,
				grpc.Header(&headers), grpc.Trailer(&trailers)).Times(1).Return(nil, nil),
		)
		client := ClientImpl{
			grpcClient:                   grpcClient,
			grpcProjectionsClientFactory: grpcProjectionsClientFactoryInstance,
		}

		err := client.CreateProjection(ctx, options)
		require.NoError(t, err)
	})

	t.Run("Error when obtaining connection", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		errorResult := errors.NewErrorCode("some error")
		grpcClient.EXPECT().GetConnectionHandle().Return(nil, errorResult)

		client := ClientImpl{
			grpcClient: grpcClient,
		}

		err := client.CreateProjection(ctx, options)
		require.Equal(t, errorResult, err)
	})

	t.Run("Error returned from grpc", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		handle := connection.NewMockConnectionHandle(ctrl)
		grpcProjectionsClientFactoryInstance := NewMockgrpcProjectionsClientFactory(ctrl)
		grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)

		errorResult := errors.NewErrorCode("some error")
		expectedHeader := metadata.MD{
			"header_key": []string{"header_value"},
		}

		expectedTrailer := metadata.MD{
			"trailer_key": []string{"trailer_value"},
		}

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
			handle.EXPECT().Connection().Return(grpcClientConn),
			grpcProjectionsClientFactoryInstance.EXPECT().Create(grpcClientConn).
				Return(grpcProjectionsClientMock),
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
			grpcClient.EXPECT().HandleError(handle, expectedHeader, expectedTrailer, errorResult,
				FailedToCreateProjectionErr).Return(errors.NewErrorCode(FailedToCreateProjectionErr)),
		)

		client := ClientImpl{
			grpcProjectionsClientFactory: grpcProjectionsClientFactoryInstance,
			grpcClient:                   grpcClient,
		}

		err := client.CreateProjection(ctx, options)
		require.Equal(t, FailedToCreateProjectionErr, err.Code())
	})
}

func TestClientImpl_UpdateProjection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()

	grpcClientConn := &grpc.ClientConn{}
	options := UpdateOptionsRequest{}
	options.SetName("some name").
		SetQuery("some query").
		SetEmitOption(UpdateOptionsEmitOptionEnabled{
			EmitEnabled: true,
		})

	grpcOptions := options.Build()

	t.Run("Success", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		handle := connection.NewMockConnectionHandle(ctrl)
		grpcProjectionsClientFactoryInstance := NewMockgrpcProjectionsClientFactory(ctrl)
		grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
			handle.EXPECT().Connection().Return(grpcClientConn),
			grpcProjectionsClientFactoryInstance.EXPECT().Create(grpcClientConn).
				Return(grpcProjectionsClientMock),
			grpcProjectionsClientMock.EXPECT().Update(ctx, grpcOptions,
				grpc.Header(&headers), grpc.Trailer(&trailers)).Times(1).Return(nil, nil),
		)

		client := ClientImpl{
			grpcClient:                   grpcClient,
			grpcProjectionsClientFactory: grpcProjectionsClientFactoryInstance,
		}

		err := client.UpdateProjection(ctx, options)
		require.NoError(t, err)
	})

	t.Run("Error when obtaining connection", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		errorResult := errors.NewErrorCode("some error")
		grpcClient.EXPECT().GetConnectionHandle().Return(nil, errorResult)

		client := ClientImpl{
			grpcClient: grpcClient,
		}

		err := client.UpdateProjection(ctx, options)
		require.Equal(t, errorResult, err)
	})

	t.Run("Error returned from grpc", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		handle := connection.NewMockConnectionHandle(ctrl)
		grpcProjectionsClientFactoryInstance := NewMockgrpcProjectionsClientFactory(ctrl)
		grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)

		errorResult := errors.NewErrorCode("some error")
		expectedHeader := metadata.MD{
			"header_key": []string{"header_value"},
		}

		expectedTrailer := metadata.MD{
			"trailer_key": []string{"trailer_value"},
		}

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
			handle.EXPECT().Connection().Return(grpcClientConn),
			grpcProjectionsClientFactoryInstance.EXPECT().Create(grpcClientConn).
				Return(grpcProjectionsClientMock),
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
			grpcClient.EXPECT().HandleError(handle, expectedHeader, expectedTrailer, errorResult,
				FailedToUpdateProjectionErr).Return(errors.NewErrorCode(FailedToUpdateProjectionErr)),
		)

		client := ClientImpl{
			grpcClient:                   grpcClient,
			grpcProjectionsClientFactory: grpcProjectionsClientFactoryInstance,
		}

		err := client.UpdateProjection(ctx, options)
		require.Equal(t, FailedToUpdateProjectionErr, err.Code())
	})
}

func TestClientImpl_DeleteProjection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()

	grpcClientConn := &grpc.ClientConn{}
	options := DeleteOptionsRequest{}
	options.SetName("some name")

	grpcOptions := options.Build()

	t.Run("Success", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		handle := connection.NewMockConnectionHandle(ctrl)
		grpcProjectionsClientFactoryInstance := NewMockgrpcProjectionsClientFactory(ctrl)
		grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
			handle.EXPECT().Connection().Return(grpcClientConn),
			grpcProjectionsClientFactoryInstance.EXPECT().Create(grpcClientConn).
				Return(grpcProjectionsClientMock),
			grpcProjectionsClientMock.EXPECT().Delete(ctx, grpcOptions,
				grpc.Header(&headers), grpc.Trailer(&trailers)).Times(1).Return(nil, nil),
		)
		client := ClientImpl{
			grpcClient:                   grpcClient,
			grpcProjectionsClientFactory: grpcProjectionsClientFactoryInstance,
		}

		err := client.DeleteProjection(ctx, options)
		require.NoError(t, err)
	})

	t.Run("Error when obtaining connection", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		errorResult := errors.NewErrorCode("some error")
		grpcClient.EXPECT().GetConnectionHandle().Return(nil, errorResult)

		client := ClientImpl{
			grpcClient: grpcClient,
		}

		err := client.DeleteProjection(ctx, options)
		require.Equal(t, errorResult, err)
	})

	t.Run("Error returned from grpc", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		handle := connection.NewMockConnectionHandle(ctrl)
		grpcProjectionsClientFactoryInstance := NewMockgrpcProjectionsClientFactory(ctrl)
		grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)

		errorResult := errors.NewErrorCode("some error")
		expectedHeader := metadata.MD{
			"header_key": []string{"header_value"},
		}

		expectedTrailer := metadata.MD{
			"trailer_key": []string{"trailer_value"},
		}

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
			handle.EXPECT().Connection().Return(grpcClientConn),
			grpcProjectionsClientFactoryInstance.EXPECT().Create(grpcClientConn).
				Return(grpcProjectionsClientMock),
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
			grpcClient.EXPECT().HandleError(handle, expectedHeader, expectedTrailer, errorResult,
				FailedToDeleteProjectionErr).Return(errors.NewErrorCode(FailedToDeleteProjectionErr)),
		)

		client := ClientImpl{
			grpcClient:                   grpcClient,
			grpcProjectionsClientFactory: grpcProjectionsClientFactoryInstance,
		}

		err := client.DeleteProjection(ctx, options)
		require.Equal(t, FailedToDeleteProjectionErr, err.Code())
	})
}

func TestClientImpl_ProjectionStatistics(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()

	grpcClientConn := &grpc.ClientConn{}
	options := StatisticsOptionsRequest{}
	options.SetMode(StatisticsOptionsRequestModeAll{})

	grpcOptions := options.Build()

	t.Run("Success", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		handle := connection.NewMockConnectionHandle(ctrl)
		grpcProjectionsClientFactoryInstance := NewMockgrpcProjectionsClientFactory(ctrl)
		grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
			handle.EXPECT().Connection().Return(grpcClientConn),
			grpcProjectionsClientFactoryInstance.EXPECT().Create(grpcClientConn).
				Return(grpcProjectionsClientMock),
			grpcProjectionsClientMock.EXPECT().Statistics(ctx, grpcOptions,
				grpc.Header(&headers), grpc.Trailer(&trailers)).Times(1).Return(nil, nil),
		)
		client := ClientImpl{
			grpcClient:                   grpcClient,
			grpcProjectionsClientFactory: grpcProjectionsClientFactoryInstance,
		}

		statisticsClient, err := client.GetProjectionStatistics(ctx, options)
		require.NotNil(t, statisticsClient)
		require.NoError(t, err)
	})

	t.Run("Error when obtaining connection", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		errorResult := errors.NewErrorCode("some error")
		grpcClient.EXPECT().GetConnectionHandle().Return(nil, errorResult)

		client := ClientImpl{
			grpcClient: grpcClient,
		}

		_, err := client.GetProjectionStatistics(ctx, options)
		require.Equal(t, errorResult, err)
	})

	t.Run("Error returned from grpc", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		handle := connection.NewMockConnectionHandle(ctrl)
		grpcProjectionsClientFactoryInstance := NewMockgrpcProjectionsClientFactory(ctrl)
		grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)

		errorResult := errors.NewErrorCode("some error")
		expectedHeader := metadata.MD{
			"header_key": []string{"header_value"},
		}

		expectedTrailer := metadata.MD{
			"trailer_key": []string{"trailer_value"},
		}

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
			handle.EXPECT().Connection().Return(grpcClientConn),
			grpcProjectionsClientFactoryInstance.EXPECT().Create(grpcClientConn).
				Return(grpcProjectionsClientMock),
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
			grpcClient.EXPECT().HandleError(handle, expectedHeader, expectedTrailer, errorResult,
				FailedToFetchProjectionStatisticsErr).Return(errors.NewErrorCode(FailedToFetchProjectionStatisticsErr)),
		)

		client := ClientImpl{
			grpcProjectionsClientFactory: grpcProjectionsClientFactoryInstance,
			grpcClient:                   grpcClient,
		}

		statisticsClient, err := client.GetProjectionStatistics(ctx, options)
		require.Nil(t, statisticsClient)
		require.Equal(t, FailedToFetchProjectionStatisticsErr, err.Code())
	})
}

func TestClientImpl_DisableProjection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()

	grpcClientConn := &grpc.ClientConn{}
	options := DisableOptionsRequest{}
	options.SetName("some name")
	grpcOptions := options.Build()

	t.Run("Success", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		handle := connection.NewMockConnectionHandle(ctrl)
		grpcProjectionsClientFactoryInstance := NewMockgrpcProjectionsClientFactory(ctrl)
		grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
			handle.EXPECT().Connection().Return(grpcClientConn),
			grpcProjectionsClientFactoryInstance.EXPECT().Create(grpcClientConn).
				Return(grpcProjectionsClientMock),
			grpcProjectionsClientMock.EXPECT().Disable(ctx, grpcOptions,
				grpc.Header(&headers), grpc.Trailer(&trailers)).Times(1).Return(nil, nil),
		)
		client := ClientImpl{
			grpcClient:                   grpcClient,
			grpcProjectionsClientFactory: grpcProjectionsClientFactoryInstance,
		}

		err := client.DisableProjection(ctx, options)
		require.NoError(t, err)
	})

	t.Run("Error when obtaining connection", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		errorResult := errors.NewErrorCode("some error")
		grpcClient.EXPECT().GetConnectionHandle().Return(nil, errorResult)

		client := ClientImpl{
			grpcClient: grpcClient,
		}

		err := client.DisableProjection(ctx, options)
		require.Equal(t, errorResult, err)
	})

	t.Run("Error returned from grpc", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		handle := connection.NewMockConnectionHandle(ctrl)
		grpcProjectionsClientFactoryInstance := NewMockgrpcProjectionsClientFactory(ctrl)
		grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)

		errorResult := errors.NewErrorCode("some error")
		expectedHeader := metadata.MD{
			"header_key": []string{"header_value"},
		}

		expectedTrailer := metadata.MD{
			"trailer_key": []string{"trailer_value"},
		}

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
			handle.EXPECT().Connection().Return(grpcClientConn),
			grpcProjectionsClientFactoryInstance.EXPECT().Create(grpcClientConn).
				Return(grpcProjectionsClientMock),
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
			grpcClient.EXPECT().HandleError(handle, expectedHeader, expectedTrailer, errorResult,
				FailedToDisableProjectionErr).Return(errors.NewErrorCode(FailedToDisableProjectionErr)),
		)

		client := ClientImpl{
			grpcProjectionsClientFactory: grpcProjectionsClientFactoryInstance,
			grpcClient:                   grpcClient,
		}

		err := client.DisableProjection(ctx, options)
		require.Equal(t, FailedToDisableProjectionErr, err.Code())
	})
}

func TestClientImpl_AbortProjection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()

	grpcClientConn := &grpc.ClientConn{}
	options := AbortOptionsRequest{}
	options.SetName("some name")
	grpcOptions := options.Build()

	t.Run("Success", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		handle := connection.NewMockConnectionHandle(ctrl)
		grpcProjectionsClientFactoryInstance := NewMockgrpcProjectionsClientFactory(ctrl)
		grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
			handle.EXPECT().Connection().Return(grpcClientConn),
			grpcProjectionsClientFactoryInstance.EXPECT().Create(grpcClientConn).
				Return(grpcProjectionsClientMock),
			grpcProjectionsClientMock.EXPECT().Disable(ctx, grpcOptions,
				grpc.Header(&headers), grpc.Trailer(&trailers)).Times(1).Return(nil, nil),
		)
		client := ClientImpl{
			grpcClient:                   grpcClient,
			grpcProjectionsClientFactory: grpcProjectionsClientFactoryInstance,
		}

		err := client.AbortProjection(ctx, options)
		require.NoError(t, err)
	})

	t.Run("Error when obtaining connection", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		errorResult := errors.NewErrorCode("some error")
		grpcClient.EXPECT().GetConnectionHandle().Return(nil, errorResult)

		client := ClientImpl{
			grpcClient: grpcClient,
		}

		err := client.AbortProjection(ctx, options)
		require.Equal(t, errorResult, err)
	})

	t.Run("Error returned from grpc", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		handle := connection.NewMockConnectionHandle(ctrl)
		grpcProjectionsClientFactoryInstance := NewMockgrpcProjectionsClientFactory(ctrl)
		grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)

		errorResult := errors.NewErrorCode("some error")
		expectedHeader := metadata.MD{
			"header_key": []string{"header_value"},
		}

		expectedTrailer := metadata.MD{
			"trailer_key": []string{"trailer_value"},
		}

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
			handle.EXPECT().Connection().Return(grpcClientConn),
			grpcProjectionsClientFactoryInstance.EXPECT().Create(grpcClientConn).
				Return(grpcProjectionsClientMock),
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
			grpcClient.EXPECT().HandleError(handle, expectedHeader, expectedTrailer, errorResult,
				FailedToAbortProjectionErr).Return(errors.NewErrorCode(FailedToAbortProjectionErr)),
		)

		client := ClientImpl{
			grpcClient:                   grpcClient,
			grpcProjectionsClientFactory: grpcProjectionsClientFactoryInstance,
		}

		err := client.AbortProjection(ctx, options)
		require.Equal(t, FailedToAbortProjectionErr, err.Code())
	})
}

func TestClientImpl_EnableProjection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()

	grpcClientConn := &grpc.ClientConn{}
	options := EnableOptionsRequest{}
	options.SetName("some name")
	grpcOptions := options.Build()

	t.Run("Success", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		handle := connection.NewMockConnectionHandle(ctrl)
		grpcProjectionsClientFactoryInstance := NewMockgrpcProjectionsClientFactory(ctrl)
		grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
			handle.EXPECT().Connection().Return(grpcClientConn),
			grpcProjectionsClientFactoryInstance.EXPECT().Create(grpcClientConn).
				Return(grpcProjectionsClientMock),
			grpcProjectionsClientMock.EXPECT().Enable(ctx, grpcOptions,
				grpc.Header(&headers), grpc.Trailer(&trailers)).Times(1).Return(nil, nil),
		)
		client := ClientImpl{
			grpcClient:                   grpcClient,
			grpcProjectionsClientFactory: grpcProjectionsClientFactoryInstance,
		}

		err := client.EnableProjection(ctx, options)
		require.NoError(t, err)
	})

	t.Run("Error when obtaining connection", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		errorResult := errors.NewErrorCode("some error")
		grpcClient.EXPECT().GetConnectionHandle().Return(nil, errorResult)

		client := ClientImpl{
			grpcClient: grpcClient,
		}

		err := client.EnableProjection(ctx, options)
		require.Equal(t, errorResult, err)
	})

	t.Run("Error returned from grpc", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		handle := connection.NewMockConnectionHandle(ctrl)
		grpcProjectionsClientFactoryInstance := NewMockgrpcProjectionsClientFactory(ctrl)
		grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)

		errorResult := errors.NewErrorCode("some error")
		expectedHeader := metadata.MD{
			"header_key": []string{"header_value"},
		}

		expectedTrailer := metadata.MD{
			"trailer_key": []string{"trailer_value"},
		}

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
			handle.EXPECT().Connection().Return(grpcClientConn),
			grpcProjectionsClientFactoryInstance.EXPECT().Create(grpcClientConn).
				Return(grpcProjectionsClientMock),
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
			grpcClient.EXPECT().HandleError(handle, expectedHeader, expectedTrailer, errorResult,
				FailedToEnableProjectionErr).Return(errors.NewErrorCode(FailedToEnableProjectionErr)),
		)

		client := ClientImpl{
			grpcClient:                   grpcClient,
			grpcProjectionsClientFactory: grpcProjectionsClientFactoryInstance,
		}
		err := client.EnableProjection(ctx, options)
		require.Equal(t, FailedToEnableProjectionErr, err.Code())
	})
}

func TestClientImpl_ResetProjection(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()

	grpcClientConn := &grpc.ClientConn{}
	options := ResetOptionsRequest{}
	options.SetName("some name").SetWriteCheckpoint(true)
	grpcOptions := options.Build()

	t.Run("Success", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		handle := connection.NewMockConnectionHandle(ctrl)
		grpcProjectionsClientFactoryInstance := NewMockgrpcProjectionsClientFactory(ctrl)
		grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
			handle.EXPECT().Connection().Return(grpcClientConn),
			grpcProjectionsClientFactoryInstance.EXPECT().Create(grpcClientConn).
				Return(grpcProjectionsClientMock),
			grpcProjectionsClientMock.EXPECT().Reset(ctx, grpcOptions,
				grpc.Header(&headers), grpc.Trailer(&trailers)).Times(1).Return(nil, nil),
		)

		client := ClientImpl{
			grpcClient:                   grpcClient,
			grpcProjectionsClientFactory: grpcProjectionsClientFactoryInstance,
		}

		err := client.ResetProjection(ctx, options)
		require.NoError(t, err)
	})

	t.Run("Error when obtaining connection", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		errorResult := errors.NewErrorCode("some error")
		grpcClient.EXPECT().GetConnectionHandle().Return(nil, errorResult)

		client := ClientImpl{
			grpcClient: grpcClient,
		}

		err := client.ResetProjection(ctx, options)
		require.Equal(t, errorResult, err)
	})

	t.Run("Error returned from grpc", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		handle := connection.NewMockConnectionHandle(ctrl)
		grpcProjectionsClientFactoryInstance := NewMockgrpcProjectionsClientFactory(ctrl)
		grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)

		errorResult := errors.NewErrorCode("some error")
		expectedHeader := metadata.MD{
			"header_key": []string{"header_value"},
		}

		expectedTrailer := metadata.MD{
			"trailer_key": []string{"trailer_value"},
		}

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
			handle.EXPECT().Connection().Return(grpcClientConn),
			grpcProjectionsClientFactoryInstance.EXPECT().Create(grpcClientConn).
				Return(grpcProjectionsClientMock),
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
			grpcClient.EXPECT().HandleError(handle, expectedHeader, expectedTrailer, errorResult,
				FailedToResetProjectionErr).Return(errors.NewErrorCode(FailedToResetProjectionErr)),
		)

		client := ClientImpl{
			grpcClient:                   grpcClient,
			grpcProjectionsClientFactory: grpcProjectionsClientFactoryInstance,
		}

		err := client.ResetProjection(ctx, options)
		require.Equal(t, FailedToResetProjectionErr, err.Code())
	})
}

func TestClientImpl_ProjectionState(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()

	grpcClientConn := &grpc.ClientConn{}
	options := StateOptionsRequest{}
	options.SetName("some name").SetPartition("some partition")
	grpcOptions := options.Build()

	response := &projections.StateResp{
		State: &structpb.Value{
			Kind: &structpb.Value_NullValue{},
		},
	}

	t.Run("Success", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		handle := connection.NewMockConnectionHandle(ctrl)
		grpcProjectionsClientFactoryInstance := NewMockgrpcProjectionsClientFactory(ctrl)
		grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
			handle.EXPECT().Connection().Return(grpcClientConn),
			grpcProjectionsClientFactoryInstance.EXPECT().Create(grpcClientConn).
				Return(grpcProjectionsClientMock),
			grpcProjectionsClientMock.EXPECT().State(ctx, grpcOptions,
				grpc.Header(&headers), grpc.Trailer(&trailers)).Times(1).Return(response, nil),
		)

		client := ClientImpl{
			grpcClient:                   grpcClient,
			grpcProjectionsClientFactory: grpcProjectionsClientFactoryInstance,
		}

		expectedStateResponse := newStateResponse(response)

		state, err := client.GetProjectionState(ctx, options)
		require.Equal(t, expectedStateResponse, state)
		require.NoError(t, err)
	})

	t.Run("Error when obtaining connection", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		errorResult := errors.NewErrorCode("some error")
		grpcClient.EXPECT().GetConnectionHandle().Return(nil, errorResult)

		client := ClientImpl{
			grpcClient: grpcClient,
		}

		_, err := client.GetProjectionState(ctx, options)
		require.Equal(t, errorResult, err)
	})

	t.Run("Error returned from grpc", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		handle := connection.NewMockConnectionHandle(ctrl)
		grpcProjectionsClientFactoryInstance := NewMockgrpcProjectionsClientFactory(ctrl)
		grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)

		errorResult := errors.NewErrorCode("some error")
		expectedHeader := metadata.MD{
			"header_key": []string{"header_value"},
		}

		expectedTrailer := metadata.MD{
			"trailer_key": []string{"trailer_value"},
		}

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
			handle.EXPECT().Connection().Return(grpcClientConn),
			grpcProjectionsClientFactoryInstance.EXPECT().Create(grpcClientConn).
				Return(grpcProjectionsClientMock),
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
			grpcClient.EXPECT().HandleError(handle, expectedHeader, expectedTrailer, errorResult,
				FailedToGetProjectionStateErr).Return(errors.NewErrorCode(FailedToGetProjectionStateErr)),
		)

		client := ClientImpl{
			grpcClient:                   grpcClient,
			grpcProjectionsClientFactory: grpcProjectionsClientFactoryInstance,
		}

		state, err := client.GetProjectionState(ctx, options)
		require.Nil(t, state)
		require.Equal(t, FailedToGetProjectionStateErr, err.Code())
	})
}

func TestClientImpl_ProjectionResult(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()

	grpcClientConn := &grpc.ClientConn{}
	options := ResultOptionsRequest{}
	options.SetName("some name").SetPartition("some partition")
	grpcOptions := options.Build()

	response := &projections.ResultResp{
		Result: &structpb.Value{
			Kind: &structpb.Value_NullValue{},
		},
	}

	t.Run("Success", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		handle := connection.NewMockConnectionHandle(ctrl)
		grpcProjectionsClientFactoryInstance := NewMockgrpcProjectionsClientFactory(ctrl)
		grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
			handle.EXPECT().Connection().Return(grpcClientConn),
			grpcProjectionsClientFactoryInstance.EXPECT().Create(grpcClientConn).
				Return(grpcProjectionsClientMock),
			grpcProjectionsClientMock.EXPECT().Result(ctx, grpcOptions,
				grpc.Header(&headers), grpc.Trailer(&trailers)).Times(1).Return(response, nil),
		)

		client := ClientImpl{
			grpcClient:                   grpcClient,
			grpcProjectionsClientFactory: grpcProjectionsClientFactoryInstance,
		}

		expectedStateResponse := newResultResponse(response)

		state, err := client.GetProjectionResult(ctx, options)
		require.Equal(t, expectedStateResponse, state)
		require.NoError(t, err)
	})

	t.Run("Error when obtaining connection", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		errorResult := errors.NewErrorCode("some error")
		grpcClient.EXPECT().GetConnectionHandle().Return(nil, errorResult)

		client := ClientImpl{
			grpcClient: grpcClient,
		}

		_, err := client.GetProjectionResult(ctx, options)
		require.Equal(t, errorResult, err)
	})

	t.Run("Error returned from grpc", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		handle := connection.NewMockConnectionHandle(ctrl)
		grpcProjectionsClientFactoryInstance := NewMockgrpcProjectionsClientFactory(ctrl)
		grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)

		errorResult := errors.NewErrorCode("some error")
		expectedHeader := metadata.MD{
			"header_key": []string{"header_value"},
		}

		expectedTrailer := metadata.MD{
			"trailer_key": []string{"trailer_value"},
		}

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
			handle.EXPECT().Connection().Return(grpcClientConn),
			grpcProjectionsClientFactoryInstance.EXPECT().Create(grpcClientConn).
				Return(grpcProjectionsClientMock),
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
			grpcClient.EXPECT().HandleError(handle, expectedHeader, expectedTrailer, errorResult,
				FailedToGetProjectionResultErr).Return(errors.NewErrorCode(FailedToGetProjectionResultErr)),
		)

		client := ClientImpl{
			grpcClient:                   grpcClient,
			grpcProjectionsClientFactory: grpcProjectionsClientFactoryInstance,
		}

		state, err := client.GetProjectionResult(ctx, options)
		require.Nil(t, state)
		require.Equal(t, FailedToGetProjectionResultErr, err.Code())
	})
}

func TestClientImpl_RestartProjectionsSubsystem(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	grpcClientConn := &grpc.ClientConn{}
	ctx := context.Background()

	t.Run("Success with RestartSubsystem return nil", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		handle := connection.NewMockConnectionHandle(ctrl)
		grpcProjectionsClientFactoryInstance := NewMockgrpcProjectionsClientFactory(ctrl)
		grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
			handle.EXPECT().Connection().Return(grpcClientConn),
			grpcProjectionsClientFactoryInstance.EXPECT().Create(grpcClientConn).
				Return(grpcProjectionsClientMock),
			grpcProjectionsClientMock.EXPECT().RestartSubsystem(ctx, &shared.Empty{},
				grpc.Header(&headers), grpc.Trailer(&trailers)).Times(1).Return(nil, nil),
		)

		client := ClientImpl{
			grpcClient:                   grpcClient,
			grpcProjectionsClientFactory: grpcProjectionsClientFactoryInstance,
		}

		err := client.RestartProjectionsSubsystem(ctx)
		require.NoError(t, err)
	})

	t.Run("Success with RestartSubsystem return &shared.Empty{}", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		handle := connection.NewMockConnectionHandle(ctrl)
		grpcProjectionsClientFactoryInstance := NewMockgrpcProjectionsClientFactory(ctrl)
		grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
			handle.EXPECT().Connection().Return(grpcClientConn),
			grpcProjectionsClientFactoryInstance.EXPECT().Create(grpcClientConn).
				Return(grpcProjectionsClientMock),
			grpcProjectionsClientMock.EXPECT().RestartSubsystem(ctx, &shared.Empty{},
				grpc.Header(&headers), grpc.Trailer(&trailers)).Times(1).Return(&shared.Empty{}, nil),
		)

		client := ClientImpl{
			grpcClient:                   grpcClient,
			grpcProjectionsClientFactory: grpcProjectionsClientFactoryInstance,
		}

		err := client.RestartProjectionsSubsystem(ctx)
		require.NoError(t, err)
	})

	t.Run("Error when obtaining connection", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		errorResult := errors.NewErrorCode("some error")
		grpcClient.EXPECT().GetConnectionHandle().Return(nil, errorResult)

		client := ClientImpl{
			grpcClient: grpcClient,
		}

		err := client.RestartProjectionsSubsystem(ctx)
		require.Equal(t, errorResult, err)
	})

	t.Run("Error returned from grpc", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		handle := connection.NewMockConnectionHandle(ctrl)
		grpcProjectionsClientFactoryInstance := NewMockgrpcProjectionsClientFactory(ctrl)
		grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)

		errorResult := errors.NewErrorCode("some error")
		expectedHeader := metadata.MD{
			"header_key": []string{"header_value"},
		}

		expectedTrailer := metadata.MD{
			"trailer_key": []string{"trailer_value"},
		}

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
			handle.EXPECT().Connection().Return(grpcClientConn),
			grpcProjectionsClientFactoryInstance.EXPECT().Create(grpcClientConn).
				Return(grpcProjectionsClientMock),
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
			grpcClient.EXPECT().HandleError(handle, expectedHeader, expectedTrailer, errorResult,
				FailedToRestartProjectionsSubsystemErr).Return(
				errors.NewErrorCode(FailedToRestartProjectionsSubsystemErr)),
		)

		client := ClientImpl{
			grpcClient:                   grpcClient,
			grpcProjectionsClientFactory: grpcProjectionsClientFactoryInstance,
		}

		err := client.RestartProjectionsSubsystem(ctx)
		require.Equal(t, FailedToRestartProjectionsSubsystemErr, err.Code())
	})
}

func TestClientImpl_ListAllProjections(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()

	grpcClientConn := &grpc.ClientConn{}
	options := StatisticsOptionsRequest{}
	options.SetMode(StatisticsOptionsRequestModeAll{})

	grpcOptions := options.Build()

	t.Run("Success", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		handle := connection.NewMockConnectionHandle(ctrl)
		grpcProjectionsClientFactoryInstance := NewMockgrpcProjectionsClientFactory(ctrl)
		grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)
		statisticsClient := projections.NewMockProjections_StatisticsClient(ctrl)

		responseList := []*projections.StatisticsResp{
			{
				Details: &projections.StatisticsResp_Details{
					Name: "response 1",
				},
			},
			{
				Details: &projections.StatisticsResp_Details{
					Name: "response 2",
				},
			},
		}

		for _, item := range responseList {
			statisticsClient.EXPECT().Recv().Return(item, nil)
		}

		statisticsClient.EXPECT().Recv().Return(nil, io.EOF)

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
			handle.EXPECT().Connection().Return(grpcClientConn),
			grpcProjectionsClientFactoryInstance.EXPECT().Create(grpcClientConn).
				Return(grpcProjectionsClientMock),
			grpcProjectionsClientMock.EXPECT().Statistics(ctx, grpcOptions,
				grpc.Header(&headers), grpc.Trailer(&trailers)).Times(1).Return(statisticsClient, nil),
		)

		client := ClientImpl{
			grpcClient:                   grpcClient,
			grpcProjectionsClientFactory: grpcProjectionsClientFactoryInstance,
		}

		allProjectionsResult, err := client.ListAllProjections(ctx)
		require.NoError(t, err)
		require.NotNil(t, allProjectionsResult)
		require.Len(t, allProjectionsResult, len(responseList))
	})

	t.Run("Error returned from Statistics", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		handle := connection.NewMockConnectionHandle(ctrl)
		grpcProjectionsClientFactoryInstance := NewMockgrpcProjectionsClientFactory(ctrl)
		grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)

		errorResult := errors.NewErrorCode("some error")
		expectedHeader := metadata.MD{
			"header_key": []string{"header_value"},
		}

		expectedTrailer := metadata.MD{
			"trailer_key": []string{"trailer_value"},
		}

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
			handle.EXPECT().Connection().Return(grpcClientConn),
			grpcProjectionsClientFactoryInstance.EXPECT().Create(grpcClientConn).
				Return(grpcProjectionsClientMock),
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
			grpcClient.EXPECT().HandleError(handle, expectedHeader, expectedTrailer, errorResult,
				FailedToFetchProjectionStatisticsErr).Return(
				errors.NewErrorCode(FailedToFetchProjectionStatisticsErr)),
		)

		client := ClientImpl{
			grpcClient:                   grpcClient,
			grpcProjectionsClientFactory: grpcProjectionsClientFactoryInstance,
		}

		allProjectionsResult, err := client.ListAllProjections(ctx)
		require.Error(t, err)
		require.Nil(t, allProjectionsResult)
		require.Equal(t, FailedToFetchProjectionStatisticsErr, err.Code())
	})

	t.Run("Error returned from statistics client read", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		handle := connection.NewMockConnectionHandle(ctrl)
		grpcProjectionsClientFactoryInstance := NewMockgrpcProjectionsClientFactory(ctrl)
		grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)
		statisticsClient := projections.NewMockProjections_StatisticsClient(ctrl)

		errorResult := errors.NewErrorCode("some error")

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
			handle.EXPECT().Connection().Return(grpcClientConn),
			grpcProjectionsClientFactoryInstance.EXPECT().Create(grpcClientConn).
				Return(grpcProjectionsClientMock),
			grpcProjectionsClientMock.EXPECT().Statistics(ctx, grpcOptions,
				grpc.Header(&headers), grpc.Trailer(&trailers)).Times(1).Return(statisticsClient, nil),
			statisticsClient.EXPECT().Recv().Return(nil, errorResult),
		)

		client := ClientImpl{
			grpcClient:                   grpcClient,
			grpcProjectionsClientFactory: grpcProjectionsClientFactoryInstance,
		}

		allProjectionsResult, err := client.ListAllProjections(ctx)
		require.Error(t, err)
		require.Equal(t, FailedToReadStatistics, err.Code())
		require.Nil(t, allProjectionsResult)
	})
}

func TestClientImpl_ListContinuousProjections(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	grpcClientConn := &grpc.ClientConn{}
	ctx := context.Background()

	options := StatisticsOptionsRequest{}
	options.SetMode(StatisticsOptionsRequestModeContinuous{})

	grpcOptions := options.Build()

	t.Run("Success", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		handle := connection.NewMockConnectionHandle(ctrl)
		grpcProjectionsClientFactoryInstance := NewMockgrpcProjectionsClientFactory(ctrl)
		grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)
		statisticsClient := projections.NewMockProjections_StatisticsClient(ctrl)

		var headers, trailers metadata.MD
		responseList := []*projections.StatisticsResp{
			{
				Details: &projections.StatisticsResp_Details{
					Name: "response 1",
				},
			},
			{
				Details: &projections.StatisticsResp_Details{
					Name: "response 2",
				},
			},
		}

		gomock.InOrder(
			grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
			handle.EXPECT().Connection().Return(grpcClientConn),
			grpcProjectionsClientFactoryInstance.EXPECT().Create(grpcClientConn).
				Return(grpcProjectionsClientMock),
			statisticsClient.EXPECT().Recv().Return(responseList[0], nil),
			statisticsClient.EXPECT().Recv().Return(responseList[1], nil),
			statisticsClient.EXPECT().Recv().Return(nil, io.EOF),
		)

		grpcProjectionsClientMock.EXPECT().Statistics(ctx, grpcOptions,
			grpc.Header(&headers), grpc.Trailer(&trailers)).Times(1).Return(statisticsClient, nil)

		client := ClientImpl{
			grpcClient:                   grpcClient,
			grpcProjectionsClientFactory: grpcProjectionsClientFactoryInstance,
		}

		projectionsResult, err := client.ListContinuousProjections(ctx)
		require.NoError(t, err)
		require.NotNil(t, projectionsResult)
		require.Len(t, projectionsResult, len(responseList))
	})

	t.Run("Error returned from Statistics", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		handle := connection.NewMockConnectionHandle(ctrl)
		grpcProjectionsClientFactoryInstance := NewMockgrpcProjectionsClientFactory(ctrl)
		grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)

		errorResult := errors.NewErrorCode("some error")
		expectedHeader := metadata.MD{
			"header_key": []string{"header_value"},
		}

		expectedTrailer := metadata.MD{
			"trailer_key": []string{"trailer_value"},
		}

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
			handle.EXPECT().Connection().Return(grpcClientConn),
			grpcProjectionsClientFactoryInstance.EXPECT().Create(grpcClientConn).
				Return(grpcProjectionsClientMock),
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
			grpcClient.EXPECT().HandleError(handle, expectedHeader, expectedTrailer, errorResult,
				FailedToFetchProjectionStatisticsErr).Return(
				errors.NewErrorCode(FailedToFetchProjectionStatisticsErr)),
		)

		client := ClientImpl{
			grpcClient:                   grpcClient,
			grpcProjectionsClientFactory: grpcProjectionsClientFactoryInstance,
		}

		allProjectionsResult, err := client.ListContinuousProjections(ctx)
		require.Error(t, err)
		require.Nil(t, allProjectionsResult)
		require.Equal(t, FailedToFetchProjectionStatisticsErr, err.Code())
	})

	t.Run("Error returned from statistics client read", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		handle := connection.NewMockConnectionHandle(ctrl)
		grpcProjectionsClientFactoryInstance := NewMockgrpcProjectionsClientFactory(ctrl)
		grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)
		statisticsClient := projections.NewMockProjections_StatisticsClient(ctrl)

		errorResult := errors.NewErrorCode("some error")

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
			handle.EXPECT().Connection().Return(grpcClientConn),
			grpcProjectionsClientFactoryInstance.EXPECT().Create(grpcClientConn).
				Return(grpcProjectionsClientMock),
			grpcProjectionsClientMock.EXPECT().Statistics(ctx, grpcOptions,
				grpc.Header(&headers), grpc.Trailer(&trailers)).Times(1).Return(statisticsClient, nil),
			statisticsClient.EXPECT().Recv().Return(nil, errorResult),
		)

		client := ClientImpl{
			grpcClient:                   grpcClient,
			grpcProjectionsClientFactory: grpcProjectionsClientFactoryInstance,
		}

		allProjectionsResult, err := client.ListContinuousProjections(ctx)
		require.Error(t, err)
		require.Equal(t, FailedToReadStatistics, err.Code())
		require.Nil(t, allProjectionsResult)
	})
}

func TestClientImpl_ListOneTimeProjections(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	grpcClientConn := &grpc.ClientConn{}
	ctx := context.Background()

	options := StatisticsOptionsRequest{}
	options.SetMode(StatisticsOptionsRequestModeOneTime{})

	grpcOptions := options.Build()

	t.Run("Success", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		handle := connection.NewMockConnectionHandle(ctrl)
		grpcProjectionsClientFactoryInstance := NewMockgrpcProjectionsClientFactory(ctrl)
		grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)
		statisticsClient := projections.NewMockProjections_StatisticsClient(ctrl)

		var headers, trailers metadata.MD
		responseList := []*projections.StatisticsResp{
			{
				Details: &projections.StatisticsResp_Details{
					Name: "response 1",
				},
			},
			{
				Details: &projections.StatisticsResp_Details{
					Name: "response 2",
				},
			},
		}

		gomock.InOrder(
			grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
			handle.EXPECT().Connection().Return(grpcClientConn),
			grpcProjectionsClientFactoryInstance.EXPECT().Create(grpcClientConn).
				Return(grpcProjectionsClientMock),
			grpcProjectionsClientMock.EXPECT().Statistics(ctx, grpcOptions,
				grpc.Header(&headers), grpc.Trailer(&trailers)).Times(1).Return(statisticsClient, nil),

			statisticsClient.EXPECT().Recv().Return(responseList[0], nil),
			statisticsClient.EXPECT().Recv().Return(responseList[1], nil),
			statisticsClient.EXPECT().Recv().Return(nil, io.EOF),
		)

		client := ClientImpl{
			grpcClient:                   grpcClient,
			grpcProjectionsClientFactory: grpcProjectionsClientFactoryInstance,
		}

		projectionsResult, err := client.ListOneTimeProjections(ctx)
		require.NoError(t, err)
		require.NotNil(t, projectionsResult)
		require.Len(t, projectionsResult, len(responseList))
	})

	t.Run("Error returned from Statistics", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		handle := connection.NewMockConnectionHandle(ctrl)
		grpcProjectionsClientFactoryInstance := NewMockgrpcProjectionsClientFactory(ctrl)
		grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)

		errorResult := errors.NewErrorCode("some error")
		expectedHeader := metadata.MD{
			"header_key": []string{"header_value"},
		}

		expectedTrailer := metadata.MD{
			"trailer_key": []string{"trailer_value"},
		}

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
			handle.EXPECT().Connection().Return(grpcClientConn),
			grpcProjectionsClientFactoryInstance.EXPECT().Create(grpcClientConn).
				Return(grpcProjectionsClientMock),
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
			grpcClient.EXPECT().HandleError(handle, expectedHeader, expectedTrailer, errorResult,
				FailedToFetchProjectionStatisticsErr).Return(errors.NewErrorCode(FailedToFetchProjectionStatisticsErr)),
		)

		client := ClientImpl{
			grpcClient:                   grpcClient,
			grpcProjectionsClientFactory: grpcProjectionsClientFactoryInstance,
		}

		allProjectionsResult, err := client.ListOneTimeProjections(ctx)
		require.Error(t, err)
		require.Nil(t, allProjectionsResult)
		require.Equal(t, FailedToFetchProjectionStatisticsErr, err.Code())
	})

	t.Run("Error returned from statistics client read", func(t *testing.T) {
		grpcClient := connection.NewMockGrpcClient(ctrl)
		handle := connection.NewMockConnectionHandle(ctrl)
		grpcProjectionsClientFactoryInstance := NewMockgrpcProjectionsClientFactory(ctrl)
		grpcProjectionsClientMock := projections.NewMockProjectionsClient(ctrl)
		statisticsClient := projections.NewMockProjections_StatisticsClient(ctrl)

		errorResult := errors.NewErrorCode("some error")
		statisticsClient.EXPECT().Recv().Return(nil, errorResult)

		var headers, trailers metadata.MD

		gomock.InOrder(
			grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
			handle.EXPECT().Connection().Return(grpcClientConn),
			grpcProjectionsClientFactoryInstance.EXPECT().Create(grpcClientConn).
				Return(grpcProjectionsClientMock),

			grpcProjectionsClientMock.EXPECT().Statistics(ctx, grpcOptions,
				grpc.Header(&headers), grpc.Trailer(&trailers)).Times(1).Return(statisticsClient, nil),
		)

		client := ClientImpl{
			grpcClient:                   grpcClient,
			grpcProjectionsClientFactory: grpcProjectionsClientFactoryInstance,
		}

		allProjectionsResult, err := client.ListOneTimeProjections(ctx)
		require.Error(t, err)
		require.Equal(t, FailedToReadStatistics, err.Code())
		require.Nil(t, allProjectionsResult)
	})
}
