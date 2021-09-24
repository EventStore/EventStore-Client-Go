package persistent

import (
	"context"
	"testing"

	"github.com/pivonroll/EventStore-Client-Go/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/pivonroll/EventStore-Client-Go/connection"

	"github.com/golang/mock/gomock"
	"github.com/pivonroll/EventStore-Client-Go/protos/persistent"
	"github.com/stretchr/testify/require"
)

func Test_Client_CreateSyncConnection_Success(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()
	var bufferSize int32 = 2
	groupName := "group 1"
	streamName := "stream name"

	protoSendRequest := toPersistentReadRequest(bufferSize, groupName, streamName)

	grpcClient := connection.NewMockGrpcClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)
	grpcSubscriptionClientFactoryInstance := NewMockgrpcSubscriptionClientFactory(ctrl)
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)
	persistentReadClient := persistent.NewMockPersistentSubscriptions_ReadClient(ctrl)
	expectedSyncReadConnection := NewMockSyncReadConnection(ctrl)
	syncReadConnectionFactory := NewMockSyncReadConnectionFactory(ctrl)
	messageAdapterProviderInstance := NewMockmessageAdapterProvider(ctrl)

	subscriptionId := "subscription ID"
	protoReadResponse := &persistent.ReadResp{
		Content: &persistent.ReadResp_SubscriptionConfirmation_{
			SubscriptionConfirmation: &persistent.ReadResp_SubscriptionConfirmation{
				SubscriptionId: subscriptionId,
			},
		},
	}

	grpcClientConn := &grpc.ClientConn{}
	messageAdapterInstance := messageAdapterImpl{}
	var headers, trailers metadata.MD
	cancelCtx, _ := context.WithCancel(ctx)

	gomock.InOrder(
		grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
		handle.EXPECT().Connection().Return(grpcClientConn),
		grpcSubscriptionClientFactoryInstance.EXPECT().Create(grpcClientConn).
			Return(persistentSubscriptionClient),
		persistentSubscriptionClient.EXPECT().Read(cancelCtx, grpc.Header(&headers), grpc.Trailer(&trailers)).
			Return(persistentReadClient, nil),
		persistentReadClient.EXPECT().Send(protoSendRequest).Return(nil),
		persistentReadClient.EXPECT().Recv().Return(protoReadResponse, nil),
		messageAdapterProviderInstance.EXPECT().GetMessageAdapter().Return(messageAdapterInstance),
		syncReadConnectionFactory.EXPECT().
			NewSyncReadConnection(persistentReadClient, subscriptionId, messageAdapterInstance, gomock.Any()).
			Return(expectedSyncReadConnection),
	)

	client := clientImpl{
		grpcClient:                    grpcClient,
		syncReadConnectionFactory:     syncReadConnectionFactory,
		messageAdapterProvider:        messageAdapterProviderInstance,
		grpcSubscriptionClientFactory: grpcSubscriptionClientFactoryInstance,
	}

	resultSyncReadConnection, err := client.SubscribeToStreamSync(ctx, bufferSize, groupName, streamName)
	require.NoError(t, err)
	require.Equal(t, expectedSyncReadConnection, resultSyncReadConnection)
}

func Test_Client_CreateSyncConnection_GetHandleConnectionError(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()
	var bufferSize int32 = 2
	groupName := "group 1"
	streamName := "stream name"

	grpcClient := connection.NewMockGrpcClient(ctrl)

	expectedError := errors.NewErrorCode("new error")
	grpcClient.EXPECT().GetConnectionHandle().Return(nil, expectedError)

	client := clientImpl{
		grpcClient: grpcClient,
	}

	_, err := client.SubscribeToStreamSync(ctx, bufferSize, groupName, streamName)
	require.Equal(t, expectedError, err)
}

func Test_Client_CreateSyncConnection_SubscriptionClientReadErr(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()
	var bufferSize int32 = 2
	groupName := "group 1"
	streamName := "stream name"

	grpcClient := connection.NewMockGrpcClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)
	grpcSubscriptionClientFactoryInstance := NewMockgrpcSubscriptionClientFactory(ctrl)
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)

	grpcClientConn := &grpc.ClientConn{}
	readError := errors.NewErrorCode("new error")
	expectedError := errors.NewErrorCode(SubscribeToStreamSync_FailedToInitPersistentSubscriptionClientErr)
	var headers, trailers metadata.MD
	cancelCtx, _ := context.WithCancel(ctx)
	expectedHeader := metadata.MD{
		"header_key": []string{"header_value"},
	}

	expectedTrailer := metadata.MD{
		"trailer_key": []string{"trailer_value"},
	}

	gomock.InOrder(
		grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
		handle.EXPECT().Connection().Return(grpcClientConn),
		grpcSubscriptionClientFactoryInstance.EXPECT().Create(grpcClientConn).
			Return(persistentSubscriptionClient),
		persistentSubscriptionClient.EXPECT().Read(cancelCtx,
			grpc.Header(&headers), grpc.Trailer(&trailers)).
			DoAndReturn(func(
				_ctx context.Context,
				options ...grpc.CallOption) (persistent.PersistentSubscriptions_ReadClient, errors.Error) {

				*options[0].(grpc.HeaderCallOption).HeaderAddr = metadata.MD{
					"header_key": []string{"header_value"},
				}

				*options[1].(grpc.TrailerCallOption).TrailerAddr = metadata.MD{
					"trailer_key": []string{"trailer_value"},
				}
				return nil, readError
			}),
		grpcClient.EXPECT().HandleError(handle, expectedHeader, expectedTrailer, readError,
			SubscribeToStreamSync_FailedToInitPersistentSubscriptionClientErr).Return(expectedError),
	)
	client := clientImpl{
		grpcClient:                    grpcClient,
		grpcSubscriptionClientFactory: grpcSubscriptionClientFactoryInstance,
	}

	_, err := client.SubscribeToStreamSync(ctx, bufferSize, groupName, streamName)
	require.Equal(t, expectedError, err)
}

func Test_Client_CreateSyncConnection_SubscriptionClientSendStreamInitializationErr(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()
	var bufferSize int32 = 2
	groupName := "group 1"
	streamName := "stream name"

	protoSendRequest := toPersistentReadRequest(bufferSize, groupName, streamName)

	grpcClient := connection.NewMockGrpcClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)
	grpcSubscriptionClientFactoryInstance := NewMockgrpcSubscriptionClientFactory(ctrl)
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)
	persistentReadClient := persistent.NewMockPersistentSubscriptions_ReadClient(ctrl)

	grpcClientConn := &grpc.ClientConn{}
	expectedError := errors.NewErrorCode("new error")

	var headers, trailers metadata.MD
	cancelCtx, _ := context.WithCancel(ctx)
	gomock.InOrder(
		grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
		handle.EXPECT().Connection().Return(grpcClientConn),
		grpcSubscriptionClientFactoryInstance.EXPECT().Create(grpcClientConn).
			Return(persistentSubscriptionClient),
		persistentSubscriptionClient.EXPECT().Read(cancelCtx, grpc.Header(&headers), grpc.Trailer(&trailers)).
			Return(persistentReadClient, nil),
		persistentReadClient.EXPECT().Send(protoSendRequest).Return(expectedError),
	)

	client := clientImpl{
		grpcClient:                    grpcClient,
		grpcSubscriptionClientFactory: grpcSubscriptionClientFactoryInstance,
	}

	_, err := client.SubscribeToStreamSync(ctx, bufferSize, groupName, streamName)
	require.Equal(t, SubscribeToStreamSync_FailedToSendStreamInitializationErr, err.Code())
}

func Test_Client_CreateSyncConnection_SubscriptionClientReceiveStreamInitializationErr(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()
	var bufferSize int32 = 2
	groupName := "group 1"
	streamName := "stream name"

	protoSendRequest := toPersistentReadRequest(bufferSize, groupName, streamName)

	grpcClient := connection.NewMockGrpcClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)
	grpcSubscriptionClientFactoryInstance := NewMockgrpcSubscriptionClientFactory(ctrl)
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)
	persistentReadClient := persistent.NewMockPersistentSubscriptions_ReadClient(ctrl)

	grpcClientConn := &grpc.ClientConn{}
	readError := errors.NewErrorCode("new error")
	expectdError := errors.NewErrorCode("expected error")
	expectedHeader := metadata.MD{
		"header_key": []string{"header_value"},
	}

	expectedTrailer := metadata.MD{
		"trailer_key": []string{"trailer_value"},
	}

	var headers, trailers metadata.MD
	cancelCtx, _ := context.WithCancel(ctx)
	gomock.InOrder(
		grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
		handle.EXPECT().Connection().Return(grpcClientConn),
		grpcSubscriptionClientFactoryInstance.EXPECT().Create(grpcClientConn).
			Return(persistentSubscriptionClient),
		persistentSubscriptionClient.EXPECT().Read(cancelCtx,
			grpc.Header(&headers), grpc.Trailer(&trailers)).
			DoAndReturn(func(
				_ctx context.Context,
				options ...grpc.CallOption) (persistent.PersistentSubscriptions_ReadClient, errors.Error) {

				*options[0].(grpc.HeaderCallOption).HeaderAddr = metadata.MD{
					"header_key": []string{"header_value"},
				}

				*options[1].(grpc.TrailerCallOption).TrailerAddr = metadata.MD{
					"trailer_key": []string{"trailer_value"},
				}
				return persistentReadClient, nil
			}),
		persistentReadClient.EXPECT().Send(protoSendRequest).Return(nil),
		persistentReadClient.EXPECT().Recv().Return(nil, readError),
		grpcClient.EXPECT().HandleError(handle, expectedHeader, expectedTrailer, readError,
			SubscribeToStreamSync_FailedToReceiveStreamInitializationErr).Return(expectdError),
	)

	client := clientImpl{
		grpcClient:                    grpcClient,
		grpcSubscriptionClientFactory: grpcSubscriptionClientFactoryInstance,
	}

	_, err := client.SubscribeToStreamSync(ctx, bufferSize, groupName, streamName)
	require.Equal(t, expectdError, err)
}

func Test_Client_CreateSyncConnection_NoSubscriptionConfirmationErr(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()
	var bufferSize int32 = 2
	groupName := "group 1"
	streamName := "stream name"

	protoSendRequest := toPersistentReadRequest(bufferSize, groupName, streamName)

	grpcClient := connection.NewMockGrpcClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)
	grpcSubscriptionClientFactoryInstance := NewMockgrpcSubscriptionClientFactory(ctrl)
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)
	persistentReadClient := persistent.NewMockPersistentSubscriptions_ReadClient(ctrl)

	grpcClientConn := &grpc.ClientConn{}
	protoReadResponse := &persistent.ReadResp{
		Content: &persistent.ReadResp_Event{},
	}

	var headers, trailers metadata.MD
	cancelCtx, _ := context.WithCancel(ctx)

	gomock.InOrder(
		grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
		handle.EXPECT().Connection().Return(grpcClientConn),
		grpcSubscriptionClientFactoryInstance.EXPECT().Create(grpcClientConn).
			Return(persistentSubscriptionClient),
		persistentSubscriptionClient.EXPECT().Read(cancelCtx, grpc.Header(&headers), grpc.Trailer(&trailers)).
			Return(persistentReadClient, nil),
		persistentReadClient.EXPECT().Send(protoSendRequest).Return(nil),
		persistentReadClient.EXPECT().Recv().Return(protoReadResponse, nil),
	)

	client := clientImpl{
		grpcClient:                    grpcClient,
		grpcSubscriptionClientFactory: grpcSubscriptionClientFactoryInstance,
	}

	_, err := client.SubscribeToStreamSync(ctx, bufferSize, groupName, streamName)
	require.Equal(t, SubscribeToStreamSync_NoSubscriptionConfirmationErr, err.Code())
}

func Test_Client_CreateStreamSubscription_Success(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()

	config := CreateOrUpdateStreamRequest{
		StreamName: "some name",
		GroupName:  "some group",
		Revision:   StreamRevision{Revision: 10},
		Settings:   DefaultRequestSettings,
	}

	grpcClient := connection.NewMockGrpcClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)
	grpcSubscriptionClientFactoryInstance := NewMockgrpcSubscriptionClientFactory(ctrl)
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)
	expectedProtoRequest := config.BuildCreateStreamRequest()

	grpcClientConn := &grpc.ClientConn{}
	var headers, trailers metadata.MD

	gomock.InOrder(
		grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
		handle.EXPECT().Connection().Return(grpcClientConn),
		grpcSubscriptionClientFactoryInstance.EXPECT().Create(grpcClientConn).
			Return(persistentSubscriptionClient),
		persistentSubscriptionClient.EXPECT().Create(ctx, expectedProtoRequest,
			grpc.Header(&headers), grpc.Trailer(&trailers)).Return(nil, nil),
	)

	client := clientImpl{
		grpcClient:                    grpcClient,
		grpcSubscriptionClientFactory: grpcSubscriptionClientFactoryInstance,
	}

	err := client.CreateStreamSubscription(ctx, config)
	require.NoError(t, err)
}

func Test_Client_CreateStreamSubscription_FailedToCreateSubscription(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()

	config := CreateOrUpdateStreamRequest{
		StreamName: "some name",
		GroupName:  "some group",
		Revision:   StreamRevision{Revision: 10},
		Settings:   DefaultRequestSettings,
	}

	grpcClient := connection.NewMockGrpcClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)
	grpcSubscriptionClientFactoryInstance := NewMockgrpcSubscriptionClientFactory(ctrl)
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)
	expectedProtoRequest := config.BuildCreateStreamRequest()

	grpcClientConn := &grpc.ClientConn{}
	clientError := errors.NewErrorCode("some error")
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
		grpcSubscriptionClientFactoryInstance.EXPECT().Create(grpcClientConn).
			Return(persistentSubscriptionClient),
		persistentSubscriptionClient.EXPECT().
			Create(ctx, expectedProtoRequest, grpc.Header(&headers), grpc.Trailer(&trailers)).
			DoAndReturn(func(
				_ctx context.Context,
				_protoRequest *persistent.CreateReq,
				options ...grpc.CallOption) (persistent.PersistentSubscriptions_ReadClient, errors.Error) {

				*options[0].(grpc.HeaderCallOption).HeaderAddr = metadata.MD{
					"header_key": []string{"header_value"},
				}

				*options[1].(grpc.TrailerCallOption).TrailerAddr = metadata.MD{
					"trailer_key": []string{"trailer_value"},
				}
				return nil, clientError
			}),
		grpcClient.EXPECT().HandleError(handle, expectedHeader, expectedTrailer, clientError,
			CreateStreamSubscription_FailedToCreatePermanentSubscriptionErr).Return(
			errors.NewErrorCode(CreateStreamSubscription_FailedToCreatePermanentSubscriptionErr)),
	)

	client := clientImpl{
		grpcClient:                    grpcClient,
		grpcSubscriptionClientFactory: grpcSubscriptionClientFactoryInstance,
	}

	err := client.CreateStreamSubscription(ctx, config)
	require.Equal(t, CreateStreamSubscription_FailedToCreatePermanentSubscriptionErr, err.Code())
}

func Test_Client_CreateAllSubscription_Success(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()

	config := CreateAllRequest{
		GroupName: "some group",
		Position: AllPosition{
			Commit:  10,
			Prepare: 20,
		},
		Filter: CreateRequestAllFilter{
			FilterBy:                     CreateRequestAllFilterByEventType,
			Matcher:                      CreateRequestAllFilterByRegex{Regex: "some regex"},
			Window:                       CreateRequestAllFilterWindowMax{Max: 10},
			CheckpointIntervalMultiplier: 20,
		},
		Settings: DefaultRequestSettings,
	}

	grpcClient := connection.NewMockGrpcClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)
	grpcSubscriptionClientFactoryInstance := NewMockgrpcSubscriptionClientFactory(ctrl)
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)
	expectedProtoRequest := config.Build()

	grpcClientConn := &grpc.ClientConn{}
	var headers, trailers metadata.MD

	gomock.InOrder(
		grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
		handle.EXPECT().Connection().Return(grpcClientConn),
		grpcSubscriptionClientFactoryInstance.EXPECT().Create(grpcClientConn).
			Return(persistentSubscriptionClient),
		persistentSubscriptionClient.EXPECT().Create(ctx, expectedProtoRequest,
			grpc.Header(&headers), grpc.Trailer(&trailers)).Return(nil, nil),
	)

	client := clientImpl{
		grpcClient:                    grpcClient,
		grpcSubscriptionClientFactory: grpcSubscriptionClientFactoryInstance,
	}

	err := client.CreateAllSubscription(ctx, config)
	require.NoError(t, err)
}

func Test_Client_CreateAllSubscription_CreateFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()

	config := CreateAllRequest{
		GroupName: "some group",
		Position: AllPosition{
			Commit:  10,
			Prepare: 20,
		},
		Filter: CreateRequestAllFilter{
			FilterBy:                     CreateRequestAllFilterByEventType,
			Matcher:                      CreateRequestAllFilterByRegex{Regex: "some regex"},
			Window:                       CreateRequestAllFilterWindowMax{Max: 10},
			CheckpointIntervalMultiplier: 20,
		},
		Settings: DefaultRequestSettings,
	}

	grpcClient := connection.NewMockGrpcClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)
	grpcSubscriptionClientFactoryInstance := NewMockgrpcSubscriptionClientFactory(ctrl)
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)
	expectedProtoRequest := config.Build()

	grpcClientConn := &grpc.ClientConn{}
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
		grpcSubscriptionClientFactoryInstance.EXPECT().Create(grpcClientConn).
			Return(persistentSubscriptionClient),
		persistentSubscriptionClient.EXPECT().Create(ctx, expectedProtoRequest,
			grpc.Header(&headers), grpc.Trailer(&trailers)).
			DoAndReturn(func(
				_ctx context.Context,
				_protoRequest *persistent.CreateReq,
				options ...grpc.CallOption) (persistent.PersistentSubscriptions_ReadClient, errors.Error) {

				*options[0].(grpc.HeaderCallOption).HeaderAddr = metadata.MD{
					"header_key": []string{"header_value"},
				}

				*options[1].(grpc.TrailerCallOption).TrailerAddr = metadata.MD{
					"trailer_key": []string{"trailer_value"},
				}
				return nil, errorResult
			}),
		grpcClient.EXPECT().HandleError(handle, expectedHeader, expectedTrailer, errorResult,
			CreateAllSubscription_FailedToCreatePermanentSubscriptionErr).Return(
			errors.NewErrorCode(CreateAllSubscription_FailedToCreatePermanentSubscriptionErr)),
	)

	client := clientImpl{
		grpcClient:                    grpcClient,
		grpcSubscriptionClientFactory: grpcSubscriptionClientFactoryInstance,
	}

	err := client.CreateAllSubscription(ctx, config)
	require.Equal(t, CreateAllSubscription_FailedToCreatePermanentSubscriptionErr, err.Code())
}

func Test_Client_UpdateStreamSubscription_Success(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()

	config := CreateOrUpdateStreamRequest{
		StreamName: "some name",
		GroupName:  "some group",
		Revision:   StreamRevision{Revision: 10},
		Settings:   DefaultRequestSettings,
	}

	grpcClient := connection.NewMockGrpcClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)
	grpcSubscriptionClientFactoryInstance := NewMockgrpcSubscriptionClientFactory(ctrl)
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)
	expectedProtoRequest := config.BuildUpdateStreamRequest()

	grpcClientConn := &grpc.ClientConn{}
	var headers, trailers metadata.MD

	gomock.InOrder(
		grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
		handle.EXPECT().Connection().Return(grpcClientConn),
		grpcSubscriptionClientFactoryInstance.EXPECT().Create(grpcClientConn).
			Return(persistentSubscriptionClient),
		persistentSubscriptionClient.EXPECT().Update(ctx, expectedProtoRequest,
			grpc.Header(&headers), grpc.Trailer(&trailers)).Return(nil, nil),
	)

	client := clientImpl{
		grpcClient:                    grpcClient,
		grpcSubscriptionClientFactory: grpcSubscriptionClientFactoryInstance,
	}

	err := client.UpdateStreamSubscription(ctx, config)
	require.NoError(t, err)
}

func Test_Client_UpdateStreamSubscription_Failure(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()

	config := CreateOrUpdateStreamRequest{
		StreamName: "some name",
		GroupName:  "some group",
		Revision:   StreamRevision{Revision: 10},
		Settings:   DefaultRequestSettings,
	}

	grpcClient := connection.NewMockGrpcClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)
	grpcSubscriptionClientFactoryInstance := NewMockgrpcSubscriptionClientFactory(ctrl)
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)
	expectedProtoRequest := config.BuildUpdateStreamRequest()

	grpcClientConn := &grpc.ClientConn{}
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
		grpcSubscriptionClientFactoryInstance.EXPECT().Create(grpcClientConn).
			Return(persistentSubscriptionClient),
		persistentSubscriptionClient.EXPECT().Update(ctx, expectedProtoRequest,
			grpc.Header(&headers), grpc.Trailer(&trailers)).
			DoAndReturn(func(
				_ctx context.Context,
				_protoRequest *persistent.UpdateReq,
				options ...grpc.CallOption) (persistent.PersistentSubscriptions_ReadClient, errors.Error) {

				*options[0].(grpc.HeaderCallOption).HeaderAddr = metadata.MD{
					"header_key": []string{"header_value"},
				}

				*options[1].(grpc.TrailerCallOption).TrailerAddr = metadata.MD{
					"trailer_key": []string{"trailer_value"},
				}
				return nil, errorResult
			}),
		grpcClient.EXPECT().HandleError(handle, expectedHeader, expectedTrailer, errorResult,
			UpdateStreamSubscription_FailedToUpdateErr).Return(
			errors.NewErrorCode(UpdateStreamSubscription_FailedToUpdateErr)),
	)

	client := clientImpl{
		grpcClient:                    grpcClient,
		grpcSubscriptionClientFactory: grpcSubscriptionClientFactoryInstance,
	}

	err := client.UpdateStreamSubscription(ctx, config)
	require.Equal(t, UpdateStreamSubscription_FailedToUpdateErr, err.Code())
}

func Test_Client_UpdateAllSubscription_Success(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()

	config := UpdateAllRequest{
		GroupName: "some group",
		Position:  AllPosition{Commit: 10, Prepare: 20},
		Settings:  DefaultRequestSettings,
	}

	grpcClient := connection.NewMockGrpcClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)
	grpcSubscriptionClientFactoryInstance := NewMockgrpcSubscriptionClientFactory(ctrl)
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)
	expectedProtoRequest := config.Build()

	grpcClientConn := &grpc.ClientConn{}
	var headers, trailers metadata.MD

	gomock.InOrder(
		grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
		handle.EXPECT().Connection().Return(grpcClientConn),
		grpcSubscriptionClientFactoryInstance.EXPECT().Create(grpcClientConn).
			Return(persistentSubscriptionClient),
		persistentSubscriptionClient.EXPECT().Update(ctx, expectedProtoRequest,
			grpc.Header(&headers), grpc.Trailer(&trailers)).Return(nil, nil),
	)

	client := clientImpl{
		grpcClient:                    grpcClient,
		grpcSubscriptionClientFactory: grpcSubscriptionClientFactoryInstance,
	}

	err := client.UpdateAllSubscription(ctx, config)
	require.NoError(t, err)
}

func Test_Client_UpdateAllSubscription_Failure(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()

	config := UpdateAllRequest{
		GroupName: "some group",
		Position:  AllPosition{Commit: 10, Prepare: 20},
		Settings:  DefaultRequestSettings,
	}

	grpcClient := connection.NewMockGrpcClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)
	grpcSubscriptionClientFactoryInstance := NewMockgrpcSubscriptionClientFactory(ctrl)
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)
	expectedProtoRequest := config.Build()

	grpcClientConn := &grpc.ClientConn{}
	expectedHeader := metadata.MD{
		"header_key": []string{"header_value"},
	}

	expectedTrailer := metadata.MD{
		"trailer_key": []string{"trailer_value"},
	}

	var headers, trailers metadata.MD

	errorResult := errors.NewErrorCode("some error")
	gomock.InOrder(
		grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
		handle.EXPECT().Connection().Return(grpcClientConn),
		grpcSubscriptionClientFactoryInstance.EXPECT().Create(grpcClientConn).
			Return(persistentSubscriptionClient),
		persistentSubscriptionClient.EXPECT().Update(ctx, expectedProtoRequest,
			grpc.Header(&headers), grpc.Trailer(&trailers)).
			DoAndReturn(func(
				_ctx context.Context,
				_protoRequest *persistent.UpdateReq,
				options ...grpc.CallOption) (persistent.PersistentSubscriptions_ReadClient, errors.Error) {

				*options[0].(grpc.HeaderCallOption).HeaderAddr = metadata.MD{
					"header_key": []string{"header_value"},
				}

				*options[1].(grpc.TrailerCallOption).TrailerAddr = metadata.MD{
					"trailer_key": []string{"trailer_value"},
				}
				return nil, errorResult
			}),
		grpcClient.EXPECT().HandleError(handle, expectedHeader, expectedTrailer, errorResult,
			UpdateAllSubscription_FailedToUpdateErr).Return(
			errors.NewErrorCode(UpdateAllSubscription_FailedToUpdateErr)),
	)

	client := clientImpl{
		grpcClient:                    grpcClient,
		grpcSubscriptionClientFactory: grpcSubscriptionClientFactoryInstance,
	}

	err := client.UpdateAllSubscription(ctx, config)
	require.Equal(t, UpdateAllSubscription_FailedToUpdateErr, err.Code())
}

func Test_Client_DeleteStreamSubscription_Success(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()

	config := DeleteRequest{
		StreamName: "some stream name",
		GroupName:  "some group name",
	}

	grpcClient := connection.NewMockGrpcClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)
	grpcSubscriptionClientFactoryInstance := NewMockgrpcSubscriptionClientFactory(ctrl)
	expectedProtoRequest := config.Build()
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)

	grpcClientConn := &grpc.ClientConn{}
	var headers, trailers metadata.MD

	gomock.InOrder(
		grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
		handle.EXPECT().Connection().Return(grpcClientConn),
		grpcSubscriptionClientFactoryInstance.EXPECT().Create(grpcClientConn).
			Return(persistentSubscriptionClient),
		persistentSubscriptionClient.EXPECT().Delete(ctx, expectedProtoRequest,
			grpc.Header(&headers), grpc.Trailer(&trailers)).Return(nil, nil),
	)

	client := clientImpl{
		grpcClient:                    grpcClient,
		grpcSubscriptionClientFactory: grpcSubscriptionClientFactoryInstance,
	}

	err := client.DeleteStreamSubscription(ctx, config)
	require.NoError(t, err)
}

func Test_Client_DeleteStreamSubscription_Failure(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()

	config := DeleteRequest{
		StreamName: "some stream name",
		GroupName:  "some group name",
	}

	grpcClient := connection.NewMockGrpcClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)
	grpcSubscriptionClientFactoryInstance := NewMockgrpcSubscriptionClientFactory(ctrl)
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)
	expectedProtoRequest := config.Build()

	grpcClientConn := &grpc.ClientConn{}
	expectedHeader := metadata.MD{
		"header_key": []string{"header_value"},
	}

	expectedTrailer := metadata.MD{
		"trailer_key": []string{"trailer_value"},
	}

	var headers, trailers metadata.MD
	errorResult := errors.NewErrorCode("some error")

	gomock.InOrder(
		grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
		handle.EXPECT().Connection().Return(grpcClientConn),
		grpcSubscriptionClientFactoryInstance.EXPECT().Create(grpcClientConn).
			Return(persistentSubscriptionClient),
		persistentSubscriptionClient.EXPECT().Delete(ctx, expectedProtoRequest,
			grpc.Header(&headers), grpc.Trailer(&trailers)).
			DoAndReturn(func(
				_ctx context.Context,
				_protoRequest *persistent.DeleteReq,
				options ...grpc.CallOption) (persistent.PersistentSubscriptions_ReadClient, errors.Error) {

				*options[0].(grpc.HeaderCallOption).HeaderAddr = metadata.MD{
					"header_key": []string{"header_value"},
				}

				*options[1].(grpc.TrailerCallOption).TrailerAddr = metadata.MD{
					"trailer_key": []string{"trailer_value"},
				}
				return nil, errorResult
			}),
		grpcClient.EXPECT().HandleError(handle, expectedHeader, expectedTrailer, errorResult,
			DeleteStreamSubscription_FailedToDeleteErr).Return(
			errors.NewErrorCode(DeleteStreamSubscription_FailedToDeleteErr)),
	)

	client := clientImpl{
		grpcClient:                    grpcClient,
		grpcSubscriptionClientFactory: grpcSubscriptionClientFactoryInstance,
	}

	err := client.DeleteStreamSubscription(ctx, config)
	require.Equal(t, DeleteStreamSubscription_FailedToDeleteErr, err.Code())
}

func Test_Client_DeleteAllSubscription_Success(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()

	grpcClient := connection.NewMockGrpcClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)
	grpcSubscriptionClientFactoryInstance := NewMockgrpcSubscriptionClientFactory(ctrl)
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)
	expectedProtoRequest := deleteRequestAllOptionsProto("some group")

	grpcClientConn := &grpc.ClientConn{}
	var headers, trailers metadata.MD

	gomock.InOrder(
		grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
		handle.EXPECT().Connection().Return(grpcClientConn),
		grpcSubscriptionClientFactoryInstance.EXPECT().Create(grpcClientConn).
			Return(persistentSubscriptionClient),
		persistentSubscriptionClient.EXPECT().Delete(ctx, expectedProtoRequest,
			grpc.Header(&headers), grpc.Trailer(&trailers)).Return(nil, nil),
	)

	client := clientImpl{
		grpcClient:                    grpcClient,
		grpcSubscriptionClientFactory: grpcSubscriptionClientFactoryInstance,
	}

	err := client.DeleteAllSubscription(ctx, "some group")
	require.NoError(t, err)
}

func Test_Client_DeleteAllSubscription_Failure(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()

	groupName := "group name"
	expectedProtoRequest := deleteRequestAllOptionsProto(groupName)

	grpcClient := connection.NewMockGrpcClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)
	grpcSubscriptionClientFactoryInstance := NewMockgrpcSubscriptionClientFactory(ctrl)
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)

	grpcClientConn := &grpc.ClientConn{}
	expectedHeader := metadata.MD{
		"header_key": []string{"header_value"},
	}

	expectedTrailer := metadata.MD{
		"trailer_key": []string{"trailer_value"},
	}

	var headers, trailers metadata.MD
	errorResult := errors.NewErrorCode("some error")

	gomock.InOrder(
		grpcClient.EXPECT().GetConnectionHandle().Return(handle, nil),
		handle.EXPECT().Connection().Return(grpcClientConn),
		grpcSubscriptionClientFactoryInstance.EXPECT().Create(grpcClientConn).
			Return(persistentSubscriptionClient),
		persistentSubscriptionClient.EXPECT().Delete(ctx, expectedProtoRequest,
			grpc.Header(&headers), grpc.Trailer(&trailers)).
			DoAndReturn(func(
				_ctx context.Context,
				_protoRequest *persistent.DeleteReq,
				options ...grpc.CallOption) (persistent.PersistentSubscriptions_ReadClient, errors.Error) {

				*options[0].(grpc.HeaderCallOption).HeaderAddr = metadata.MD{
					"header_key": []string{"header_value"},
				}

				*options[1].(grpc.TrailerCallOption).TrailerAddr = metadata.MD{
					"trailer_key": []string{"trailer_value"},
				}
				return nil, errorResult
			}),
		grpcClient.EXPECT().HandleError(handle, expectedHeader, expectedTrailer, errorResult,
			DeleteAllSubscription_FailedToDeleteErr).Return(
			errors.NewErrorCode(DeleteAllSubscription_FailedToDeleteErr)),
	)

	client := clientImpl{
		grpcClient:                    grpcClient,
		grpcSubscriptionClientFactory: grpcSubscriptionClientFactoryInstance,
	}

	err := client.DeleteAllSubscription(ctx, groupName)
	require.Equal(t, DeleteAllSubscription_FailedToDeleteErr, err.Code())
}
