package persistent

import (
	"context"
	"testing"

	"github.com/EventStore/EventStore-Client-Go/errors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/EventStore/EventStore-Client-Go/connection"

	"github.com/EventStore/EventStore-Client-Go/protos/persistent"
	"github.com/golang/mock/gomock"
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

	messageAdapterInstance := messageAdapterImpl{}
	var headers, trailers metadata.MD
	cancelCtx, _ := context.WithCancel(ctx)

	gomock.InOrder(
		persistentSubscriptionClient.EXPECT().
			Read(cancelCtx, grpc.Header(&headers), grpc.Trailer(&trailers)).
			Return(persistentReadClient, nil),
		persistentReadClient.EXPECT().Send(protoSendRequest).Return(nil),
		persistentReadClient.EXPECT().Recv().Return(protoReadResponse, nil),
		messageAdapterProviderInstance.EXPECT().GetMessageAdapter().Return(messageAdapterInstance),
		syncReadConnectionFactory.EXPECT().
			NewSyncReadConnection(persistentReadClient, subscriptionId, messageAdapterInstance, gomock.Any()).
			Return(expectedSyncReadConnection),
	)

	client := clientImpl{
		persistentSubscriptionClient: persistentSubscriptionClient,
		syncReadConnectionFactory:    syncReadConnectionFactory,
		messageAdapterProvider:       messageAdapterProviderInstance,
	}
	handle := connection.NewMockConnectionHandle(ctrl)

	resultSyncReadConnection, err := client.SubscribeToStreamSync(ctx, handle, bufferSize, groupName, streamName)
	require.NoError(t, err)
	require.Equal(t, expectedSyncReadConnection, resultSyncReadConnection)
}

func Test_Client_CreateSyncConnection_SubscriptionClientReadErr(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()
	var bufferSize int32 = 2
	groupName := "group 1"
	streamName := "stream name"

	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)

	expectedError := errors.NewErrorCode("new error")
	expectedHeader := metadata.MD{
		"header_key": []string{"header_value"},
	}

	expectedTrailer := metadata.MD{
		"trailer_key": []string{"trailer_value"},
	}

	grpcClient := connection.NewMockGrpcClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)

	var headers, trailers metadata.MD
	canceContext, _ := context.WithCancel(ctx)

	gomock.InOrder(
		persistentSubscriptionClient.EXPECT().Read(canceContext, grpc.Header(&headers), grpc.Trailer(&trailers)).
			DoAndReturn(func(
				_ctx context.Context,
				options ...grpc.CallOption) (persistent.PersistentSubscriptions_ReadClient, errors.Error) {

				*options[0].(grpc.HeaderCallOption).HeaderAddr = metadata.MD{
					"header_key": []string{"header_value"},
				}

				*options[1].(grpc.TrailerCallOption).TrailerAddr = metadata.MD{
					"trailer_key": []string{"trailer_value"},
				}
				return nil, expectedError
			}),
		grpcClient.EXPECT().HandleError(handle, expectedHeader, expectedTrailer, expectedError,
			SubscribeToStreamSync_FailedToInitPersistentSubscriptionClientErr).Return(
			errors.NewErrorCode(SubscribeToStreamSync_FailedToInitPersistentSubscriptionClientErr)),
	)

	client := clientImpl{
		persistentSubscriptionClient: persistentSubscriptionClient,
		grpcClient:                   grpcClient,
	}

	_, err := client.SubscribeToStreamSync(ctx, handle, bufferSize, groupName, streamName)
	require.Equal(t, SubscribeToStreamSync_FailedToInitPersistentSubscriptionClientErr, err.Code())
}

func Test_Client_CreateSyncConnection_SubscriptionClientSendStreamInitializationErr(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()
	var bufferSize int32 = 2
	groupName := "group 1"
	streamName := "stream name"

	protoSendRequest := toPersistentReadRequest(bufferSize, groupName, streamName)

	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)
	persistentReadClient := persistent.NewMockPersistentSubscriptions_ReadClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)
	grpcClient := connection.NewMockGrpcClient(ctrl)

	expectedError := errors.NewErrorCode("new error")

	var headers, trailers metadata.MD
	cancelCtx, _ := context.WithCancel(ctx)
	gomock.InOrder(
		persistentSubscriptionClient.EXPECT().Read(cancelCtx, grpc.Header(&headers), grpc.Trailer(&trailers)).
			Return(persistentReadClient, nil),
		persistentReadClient.EXPECT().Send(protoSendRequest).Return(expectedError),
	)

	client := clientImpl{
		persistentSubscriptionClient: persistentSubscriptionClient,
		grpcClient:                   grpcClient,
	}

	_, err := client.SubscribeToStreamSync(ctx, handle, bufferSize, groupName, streamName)
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

	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)
	persistentReadClient := persistent.NewMockPersistentSubscriptions_ReadClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)

	expectedError := errors.NewErrorCode("new error")

	var headers, trailers metadata.MD
	cancelCtx, _ := context.WithCancel(ctx)
	gomock.InOrder(
		persistentSubscriptionClient.EXPECT().Read(cancelCtx, grpc.Header(&headers), grpc.Trailer(&trailers)).
			Return(persistentReadClient, nil),
		persistentReadClient.EXPECT().Send(protoSendRequest).Return(nil),
		persistentReadClient.EXPECT().Recv().Return(nil, expectedError),
	)

	client := clientImpl{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	_, err := client.SubscribeToStreamSync(ctx, handle, bufferSize, groupName, streamName)
	require.Equal(t, SubscribeToStreamSync_FailedToReceiveStreamInitializationErr, err.Code())
}

func Test_Client_CreateSyncConnection_NoSubscriptionConfirmationErr(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()
	var bufferSize int32 = 2
	groupName := "group 1"
	streamName := "stream name"

	protoSendRequest := toPersistentReadRequest(bufferSize, groupName, streamName)

	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)
	persistentReadClient := persistent.NewMockPersistentSubscriptions_ReadClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)

	protoReadResponse := &persistent.ReadResp{
		Content: &persistent.ReadResp_Event{},
	}

	var headers, trailers metadata.MD
	cancelCtx, _ := context.WithCancel(ctx)
	gomock.InOrder(
		persistentSubscriptionClient.EXPECT().Read(cancelCtx, grpc.Header(&headers), grpc.Trailer(&trailers)).
			Return(persistentReadClient, nil),
		persistentReadClient.EXPECT().Send(protoSendRequest).Return(nil),
		persistentReadClient.EXPECT().Recv().Return(protoReadResponse, nil),
	)

	client := clientImpl{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	_, err := client.SubscribeToStreamSync(ctx, handle, bufferSize, groupName, streamName)
	require.Equal(t, SubscribeToStreamSync_NoSubscriptionConfirmationErr, err.Code())
}

func Test_Client_CreateStreamSubscription_Success(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()

	config := CreateStreamRequest{
		StreamName: "some name",
		GroupName:  "some group",
		Revision:   StreamRevision{Revision: 10},
		Settings:   DefaultRequestSettings,
	}

	expectedProtoRequest := config.Build()
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)

	var headers, trailers metadata.MD
	persistentSubscriptionClient.EXPECT().Create(ctx, expectedProtoRequest,
		grpc.Header(&headers), grpc.Trailer(&trailers)).Return(nil, nil)

	client := clientImpl{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	err := client.CreateStreamSubscription(ctx, handle, config)
	require.NoError(t, err)
}

func Test_Client_CreateStreamSubscription_FailedToCreateSubscription(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()

	config := CreateStreamRequest{
		StreamName: "some name",
		GroupName:  "some group",
		Revision:   StreamRevision{Revision: 10},
		Settings:   DefaultRequestSettings,
	}

	expectedProtoRequest := config.Build()
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)
	grpcClient := connection.NewMockGrpcClient(ctrl)

	clientError := errors.NewErrorCode("some error")
	expectedHeader := metadata.MD{
		"header_key": []string{"header_value"},
	}

	expectedTrailer := metadata.MD{
		"trailer_key": []string{"trailer_value"},
	}

	var headers, trailers metadata.MD

	gomock.InOrder(
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
		persistentSubscriptionClient: persistentSubscriptionClient,
		grpcClient:                   grpcClient,
	}

	err := client.CreateStreamSubscription(ctx, handle, config)
	require.Equal(t, CreateStreamSubscription_FailedToCreatePermanentSubscriptionErr, err.Code())
}

func Test_Client_CreateAllSubscription_Success(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()

	config := CreateRequestAll{
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

	expectedProtoRequest := config.Build()
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)

	var headers, trailers metadata.MD
	persistentSubscriptionClient.EXPECT().Create(ctx, expectedProtoRequest,
		grpc.Header(&headers), grpc.Trailer(&trailers)).Return(nil, nil)

	client := clientImpl{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	err := client.CreateAllSubscription(ctx, handle, config)
	require.NoError(t, err)
}

func Test_Client_CreateAllSubscription_CreateFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()

	config := CreateRequestAll{
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

	expectedProtoRequest := config.Build()
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)
	grpcClient := connection.NewMockGrpcClient(ctrl)

	errorResult := errors.NewErrorCode("some error")
	expectedHeader := metadata.MD{
		"header_key": []string{"header_value"},
	}

	expectedTrailer := metadata.MD{
		"trailer_key": []string{"trailer_value"},
	}

	var headers, trailers metadata.MD
	gomock.InOrder(
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
		persistentSubscriptionClient: persistentSubscriptionClient,
		grpcClient:                   grpcClient,
	}

	err := client.CreateAllSubscription(ctx, handle, config)
	require.Equal(t, CreateAllSubscription_FailedToCreatePermanentSubscriptionErr, err.Code())
}

func Test_Client_UpdateStreamSubscription_Success(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()

	config := UpdateStreamRequest{
		StreamName: "some name",
		GroupName:  "some group",
		Revision:   StreamRevision{Revision: 10},
		Settings:   DefaultRequestSettings,
	}

	expectedProtoRequest := config.Build()
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)

	var headers, trailers metadata.MD
	persistentSubscriptionClient.EXPECT().Update(ctx, expectedProtoRequest,
		grpc.Header(&headers), grpc.Trailer(&trailers)).Return(nil, nil)

	client := clientImpl{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	err := client.UpdateStreamSubscription(ctx, handle, config)
	require.NoError(t, err)
}

func Test_Client_UpdateStreamSubscription_Failure(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()

	config := UpdateStreamRequest{
		StreamName: "some name",
		GroupName:  "some group",
		Revision:   StreamRevision{Revision: 10},
		Settings:   DefaultRequestSettings,
	}

	expectedProtoRequest := config.Build()
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)
	grpcClient := connection.NewMockGrpcClient(ctrl)

	errorResult := errors.NewErrorCode("some error")
	expectedHeader := metadata.MD{
		"header_key": []string{"header_value"},
	}

	expectedTrailer := metadata.MD{
		"trailer_key": []string{"trailer_value"},
	}

	var headers, trailers metadata.MD

	gomock.InOrder(
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
		persistentSubscriptionClient: persistentSubscriptionClient,
		grpcClient:                   grpcClient,
	}

	err := client.UpdateStreamSubscription(ctx, handle, config)
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

	expectedProtoRequest := config.Build()
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)

	var headers, trailers metadata.MD
	persistentSubscriptionClient.EXPECT().Update(ctx, expectedProtoRequest,
		grpc.Header(&headers), grpc.Trailer(&trailers)).Return(nil, nil)

	client := clientImpl{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	err := client.UpdateAllSubscription(ctx, handle, config)
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

	expectedProtoRequest := config.Build()
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)
	grpcClient := connection.NewMockGrpcClient(ctrl)

	expectedHeader := metadata.MD{
		"header_key": []string{"header_value"},
	}

	expectedTrailer := metadata.MD{
		"trailer_key": []string{"trailer_value"},
	}

	var headers, trailers metadata.MD

	errorResult := errors.NewErrorCode("some error")
	gomock.InOrder(
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
		persistentSubscriptionClient: persistentSubscriptionClient,
		grpcClient:                   grpcClient,
	}

	err := client.UpdateAllSubscription(ctx, handle, config)
	require.Equal(t, UpdateAllSubscription_FailedToUpdateErr, err.Code())
}

func Test_Client_DeleteStreamSubscription_Success(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()

	config := DeleteOptions{
		StreamName: []byte("some stream name"),
		GroupName:  "some group name",
	}

	expectedProtoRequest := deleteRequestStreamProto(config)
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)

	var headers, trailers metadata.MD
	persistentSubscriptionClient.EXPECT().Delete(ctx, expectedProtoRequest,
		grpc.Header(&headers), grpc.Trailer(&trailers)).Return(nil, nil)

	client := clientImpl{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	err := client.DeleteStreamSubscription(ctx, handle, config)
	require.NoError(t, err)
}

func Test_Client_DeleteStreamSubscription_Failure(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()

	config := DeleteOptions{
		StreamName: []byte("some stream name"),
		GroupName:  "some group name",
	}

	expectedProtoRequest := deleteRequestStreamProto(config)
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)
	grpcClient := connection.NewMockGrpcClient(ctrl)

	expectedHeader := metadata.MD{
		"header_key": []string{"header_value"},
	}

	expectedTrailer := metadata.MD{
		"trailer_key": []string{"trailer_value"},
	}

	var headers, trailers metadata.MD
	errorResult := errors.NewErrorCode("some error")

	gomock.InOrder(
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
		persistentSubscriptionClient: persistentSubscriptionClient,
		grpcClient:                   grpcClient,
	}

	err := client.DeleteStreamSubscription(ctx, handle, config)
	require.Equal(t, DeleteStreamSubscription_FailedToDeleteErr, err.Code())
}

func Test_Client_DeleteAllSubscription_Success(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()

	config := SubscriptionStreamConfig{
		StreamOption: StreamSettings{
			StreamName: []byte("some name"),
			Revision:   10,
		},
		GroupName: "some group",
		Settings:  DefaultSubscriptionSettings,
	}

	expectedProtoRequest := updateRequestStreamProto(config)
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)

	var headers, trailers metadata.MD
	persistentSubscriptionClient.EXPECT().Update(ctx, expectedProtoRequest,
		grpc.Header(&headers), grpc.Trailer(&trailers)).Return(nil, nil)

	client := clientImpl{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	err := client.DeleteAllSubscription(ctx, handle, config)
	require.NoError(t, err)
}

func Test_Client_DeleteAllSubscription_Failure(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()

	groupName := "group name"
	expectedProtoRequest := deleteRequestAllOptionsProto(groupName)

	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)
	handle := connection.NewMockConnectionHandle(ctrl)
	grpcClient := connection.NewMockGrpcClient(ctrl)

	expectedHeader := metadata.MD{
		"header_key": []string{"header_value"},
	}

	expectedTrailer := metadata.MD{
		"trailer_key": []string{"trailer_value"},
	}

	var headers, trailers metadata.MD

	errorResult := errors.NewErrorCode("some error")

	gomock.InOrder(
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
		persistentSubscriptionClient: persistentSubscriptionClient,
		grpcClient:                   grpcClient,
	}

	err := client.DeleteAllSubscription(ctx, handle, groupName)
	require.Equal(t, DeleteAllSubscription_FailedToDeleteErr, err.Code())
}
