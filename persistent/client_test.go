package persistent

import (
	"context"
	"errors"
	"testing"

	"github.com/EventStore/EventStore-Client-Go/client/filtering"
	"github.com/EventStore/EventStore-Client-Go/position"
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
	streamName := []byte("stream name")

	protoSendRequest := ToPersistentReadRequest(bufferSize, groupName, streamName)

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

	gomock.InOrder(
		persistentSubscriptionClient.EXPECT().Read(ctx, []interface{}{}...).Return(persistentReadClient, nil),
		persistentReadClient.EXPECT().Send(protoSendRequest).Return(nil),
		persistentReadClient.EXPECT().Recv().Return(protoReadResponse, nil),
		messageAdapterProviderInstance.EXPECT().GetMessageAdapter().Return(messageAdapterInstance),
		syncReadConnectionFactory.EXPECT().
			NewSyncReadConnection(persistentReadClient, subscriptionId, messageAdapterInstance).
			Return(expectedSyncReadConnection),
	)

	client := clientImpl{
		persistentSubscriptionClient: persistentSubscriptionClient,
		syncReadConnectionFactory:    syncReadConnectionFactory,
		messageAdapterProvider:       messageAdapterProviderInstance,
	}

	resultSyncReadConnection, err := client.SubscribeToStreamSync(ctx, bufferSize, groupName, streamName)
	require.NoError(t, err)
	require.Equal(t, expectedSyncReadConnection, resultSyncReadConnection)
}

func Test_Client_CreateSyncConnection_SubscriptionClientReadErr(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()
	var bufferSize int32 = 2
	groupName := "group 1"
	streamName := []byte("stream name")

	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)

	expectedError := errors.New("new error")
	gomock.InOrder(
		persistentSubscriptionClient.EXPECT().Read(ctx, []interface{}{}...).Return(nil, expectedError),
	)

	client := clientImpl{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	_, err := client.SubscribeToStreamSync(ctx, bufferSize, groupName, streamName)
	require.Equal(t, SubscribeToStreamSync_FailedToInitPersistentSubscriptionClientErr, err.(Error).Code())
}

func Test_Client_CreateSyncConnection_SubscriptionClientSendStreamInitializationErr(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()
	var bufferSize int32 = 2
	groupName := "group 1"
	streamName := []byte("stream name")

	protoSendRequest := ToPersistentReadRequest(bufferSize, groupName, streamName)

	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)
	persistentReadClient := persistent.NewMockPersistentSubscriptions_ReadClient(ctrl)

	expectedError := errors.New("new error")

	gomock.InOrder(
		persistentSubscriptionClient.EXPECT().Read(ctx, []interface{}{}...).Return(persistentReadClient, nil),
		persistentReadClient.EXPECT().Send(protoSendRequest).Return(expectedError),
	)

	client := clientImpl{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	_, err := client.SubscribeToStreamSync(ctx, bufferSize, groupName, streamName)
	require.Equal(t, SubscribeToStreamSync_FailedToSendStreamInitializationErr, err.(Error).Code())
}

func Test_Client_CreateSyncConnection_SubscriptionClientReceiveStreamInitializationErr(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()
	var bufferSize int32 = 2
	groupName := "group 1"
	streamName := []byte("stream name")

	protoSendRequest := ToPersistentReadRequest(bufferSize, groupName, streamName)

	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)
	persistentReadClient := persistent.NewMockPersistentSubscriptions_ReadClient(ctrl)

	expectedError := errors.New("new error")

	gomock.InOrder(
		persistentSubscriptionClient.EXPECT().Read(ctx, []interface{}{}...).Return(persistentReadClient, nil),
		persistentReadClient.EXPECT().Send(protoSendRequest).Return(nil),
		persistentReadClient.EXPECT().Recv().Return(nil, expectedError),
	)

	client := clientImpl{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	_, err := client.SubscribeToStreamSync(ctx, bufferSize, groupName, streamName)
	require.Equal(t, SubscribeToStreamSync_FailedToReceiveStreamInitializationErr, err.(Error).Code())
}

func Test_Client_CreateSyncConnection_NoSubscriptionConfirmationErr(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()
	var bufferSize int32 = 2
	groupName := "group 1"
	streamName := []byte("stream name")

	protoSendRequest := ToPersistentReadRequest(bufferSize, groupName, streamName)

	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)
	persistentReadClient := persistent.NewMockPersistentSubscriptions_ReadClient(ctrl)

	protoReadResponse := &persistent.ReadResp{
		Content: &persistent.ReadResp_Event{},
	}

	gomock.InOrder(
		persistentSubscriptionClient.EXPECT().Read(ctx, []interface{}{}...).Return(persistentReadClient, nil),
		persistentReadClient.EXPECT().Send(protoSendRequest).Return(nil),
		persistentReadClient.EXPECT().Recv().Return(protoReadResponse, nil),
	)

	client := clientImpl{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	_, err := client.SubscribeToStreamSync(ctx, bufferSize, groupName, streamName)
	require.Equal(t, SubscribeToStreamSync_NoSubscriptionConfirmationErr, err.(Error).Code())
}

func Test_Client_CreateStreamSubscription_Success(t *testing.T) {
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

	expectedProtoRequest := CreateRequestProto(config)
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)

	persistentSubscriptionClient.EXPECT().Create(ctx, expectedProtoRequest).Return(nil, nil)

	client := clientImpl{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	err := client.CreateStreamSubscription(ctx, config)
	require.NoError(t, err)
}

func Test_Client_CreateStreamSubscription_FailedToCreateSubscription(t *testing.T) {
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

	expectedProtoRequest := CreateRequestProto(config)
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)

	clientError := errors.New("some error")
	persistentSubscriptionClient.EXPECT().Create(ctx, expectedProtoRequest).Return(nil, clientError)

	client := clientImpl{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	err := client.CreateStreamSubscription(ctx, config)
	require.Equal(t, CreateStreamSubscription_FailedToCreatePermanentSubscriptionErr, err.(Error).Code())
}

func Test_Client_CreateAllSubscription_Success(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()

	config := SubscriptionAllOptionConfig{
		Position: position.Position{
			Commit:  10,
			Prepare: 20,
		},
		Filter: &filtering.SubscriptionFilterOptions{
			MaxSearchWindow:    10,
			CheckpointInterval: 20,
			CheckpointReached:  nil,
			SubscriptionFilter: filtering.SubscriptionFilter{
				FilterType: filtering.EventFilter,
				Prefixes:   nil,
				Regex:      "some regex",
			},
		},
		GroupName: "some group",
		Settings:  DefaultSubscriptionSettings,
	}

	expectedProtoRequest, err := CreateRequestAllOptionsProto(config)
	require.NoError(t, err)
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)

	persistentSubscriptionClient.EXPECT().Create(ctx, expectedProtoRequest).Return(nil, nil)

	client := clientImpl{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	err = client.CreateAllSubscription(ctx, config)
	require.NoError(t, err)
}

func Test_Client_CreateAllSubscription_CreateFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()

	config := SubscriptionAllOptionConfig{
		Position: position.Position{
			Commit:  10,
			Prepare: 20,
		},
		Filter: &filtering.SubscriptionFilterOptions{
			MaxSearchWindow:    10,
			CheckpointInterval: 20,
			CheckpointReached:  nil,
			SubscriptionFilter: filtering.SubscriptionFilter{
				FilterType: filtering.EventFilter,
				Prefixes:   nil,
				Regex:      "some regex",
			},
		},
		GroupName: "some group",
		Settings:  DefaultSubscriptionSettings,
	}

	expectedProtoRequest, err := CreateRequestAllOptionsProto(config)
	require.NoError(t, err)
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)

	errorResult := errors.New("some error")
	persistentSubscriptionClient.EXPECT().Create(ctx, expectedProtoRequest).Return(nil, errorResult)

	client := clientImpl{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	err = client.CreateAllSubscription(ctx, config)
	require.Equal(t, CreateAllSubscription_FailedToCreatePermanentSubscriptionErr, err.(Error).Code())
}

func Test_Client_CreateAllSubscription_MustProvideRegexOrPrefix(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()

	config := SubscriptionAllOptionConfig{
		Position: position.Position{
			Commit:  10,
			Prepare: 20,
		},
		Filter: &filtering.SubscriptionFilterOptions{
			MaxSearchWindow:    10,
			CheckpointInterval: 20,
			CheckpointReached:  nil,
			SubscriptionFilter: filtering.SubscriptionFilter{
				FilterType: filtering.EventFilter,
				Prefixes:   nil,
				Regex:      "",
			},
		},
		GroupName: "some group",
		Settings:  DefaultSubscriptionSettings,
	}

	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)

	client := clientImpl{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	err := client.CreateAllSubscription(ctx, config)
	require.Equal(t, CreateAllSubscription_MustProvideRegexOrPrefixErr, err.(Error).Code())
}

func Test_Client_CreateAllSubscription_CanSetOnlyRegexOrPrefix(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()

	config := SubscriptionAllOptionConfig{
		Position: position.Position{
			Commit:  10,
			Prepare: 20,
		},
		Filter: &filtering.SubscriptionFilterOptions{
			MaxSearchWindow:    10,
			CheckpointInterval: 20,
			CheckpointReached:  nil,
			SubscriptionFilter: filtering.SubscriptionFilter{
				FilterType: filtering.EventFilter,
				Prefixes:   []string{"aaaa"},
				Regex:      "a",
			},
		},
		GroupName: "some group",
		Settings:  DefaultSubscriptionSettings,
	}

	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)

	client := clientImpl{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	err := client.CreateAllSubscription(ctx, config)
	require.Equal(t, CreateAllSubscription_CanSetOnlyRegexOrPrefixErr, err.(Error).Code())
}

func Test_Client_UpdateStreamSubscription_Success(t *testing.T) {
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

	expectedProtoRequest := UpdateRequestStreamProto(config)
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)

	persistentSubscriptionClient.EXPECT().Update(ctx, expectedProtoRequest).Return(nil, nil)

	client := clientImpl{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	err := client.UpdateStreamSubscription(ctx, config)
	require.NoError(t, err)
}

func Test_Client_UpdateStreamSubscription_Failure(t *testing.T) {
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

	expectedProtoRequest := UpdateRequestStreamProto(config)
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)

	errorResult := errors.New("some error")
	persistentSubscriptionClient.EXPECT().Update(ctx, expectedProtoRequest).Return(nil, errorResult)

	client := clientImpl{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	err := client.UpdateStreamSubscription(ctx, config)
	require.Equal(t, UpdateStreamSubscription_FailedToUpdateErr, err.(Error).Code())
}

func Test_Client_UpdateAllSubscription_Success(t *testing.T) {
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

	expectedProtoRequest := UpdateRequestStreamProto(config)
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)

	persistentSubscriptionClient.EXPECT().Update(ctx, expectedProtoRequest).Return(nil, nil)

	client := clientImpl{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	err := client.UpdateStreamSubscription(ctx, config)
	require.NoError(t, err)
}

func Test_Client_UpdateAllSubscription_UpdateFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()

	config := SubscriptionUpdateAllOptionConfig{
		Position: position.Position{
			Commit:  10,
			Prepare: 20,
		},
		GroupName: "some group",
		Settings:  DefaultSubscriptionSettings,
	}

	expectedProtoRequest := UpdateRequestAllOptionsProto(config)
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)

	errorResult := errors.New("some error")
	persistentSubscriptionClient.EXPECT().Update(ctx, expectedProtoRequest).Return(nil, errorResult)

	client := clientImpl{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	err := client.UpdateAllSubscription(ctx, config)
	require.Equal(t, UpdateAllSubscription_FailedToUpdateErr, err.(Error).Code())
}

func Test_Client_DeleteStreamSubscription_Success(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()

	config := DeleteOptions{
		StreamName: []byte("some stream name"),
		GroupName:  "some group name",
	}

	expectedProtoRequest := DeleteRequestStreamProto(config)
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)

	persistentSubscriptionClient.EXPECT().Delete(ctx, expectedProtoRequest).Return(nil, nil)

	client := clientImpl{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	err := client.DeleteStreamSubscription(ctx, config)
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

	expectedProtoRequest := DeleteRequestStreamProto(config)
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)

	errorResult := errors.New("some error")
	persistentSubscriptionClient.EXPECT().Delete(ctx, expectedProtoRequest).Return(nil, errorResult)

	client := clientImpl{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	err := client.DeleteStreamSubscription(ctx, config)
	require.Equal(t, DeleteStreamSubscription_FailedToDeleteErr, err.(Error).Code())
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

	expectedProtoRequest := UpdateRequestStreamProto(config)
	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)

	persistentSubscriptionClient.EXPECT().Update(ctx, expectedProtoRequest).Return(nil, nil)

	client := clientImpl{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	err := client.UpdateStreamSubscription(ctx, config)
	require.NoError(t, err)
}

func Test_Client_DeleteAllSubscription_UpdateFailure(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	ctx := context.Background()

	groupName := "group name"
	expectedProtoRequest := DeleteRequestAllOptionsProto(groupName)

	persistentSubscriptionClient := persistent.NewMockPersistentSubscriptionsClient(ctrl)

	errorResult := errors.New("some error")
	persistentSubscriptionClient.EXPECT().Delete(ctx, expectedProtoRequest).Return(nil, errorResult)

	client := clientImpl{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	err := client.DeleteAllSubscription(ctx, groupName)
	require.Equal(t, DeleteAllSubscription_FailedToDeleteErr, err.(Error).Code())
}
