package client

import (
	"context"
	"testing"

	"github.com/EventStore/EventStore-Client-Go/errors"
	persistent2 "github.com/EventStore/EventStore-Client-Go/protos/persistent"

	"google.golang.org/grpc"

	"github.com/EventStore/EventStore-Client-Go/connection"

	"github.com/EventStore/EventStore-Client-Go/persistent"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func Test_Client_ConnectToPersistentSubscription(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	persistentSubscriptionClientFactory := persistent.NewMockClientFactory(ctrl)
	grpcClient := connection.NewMockGrpcClient(ctrl)
	connectionHandle := connection.NewMockConnectionHandle(ctrl)
	persistentSubscriptionClient := persistent.NewMockClient(ctrl)
	expectedSyncReadConnection := persistent.NewMockSyncReadConnection(ctrl)

	ctx := context.Background()
	var bufferSize int32 = 10
	groupName := "some group name"
	streamName := "some stream"

	grpcClientConnection := &grpc.ClientConn{}
	expectedErrorResult := errors.NewErrorCode("some error")
	expectedPersistentSubscriptionsClient := persistent2.NewPersistentSubscriptionsClient(grpcClientConnection)

	persistentSubscriptionClient.EXPECT().
		SubscribeToStreamSync(ctx, connectionHandle, bufferSize, groupName, streamName).
		Return(expectedSyncReadConnection, expectedErrorResult)
	connectionHandle.EXPECT().Connection().Return(grpcClientConnection)
	grpcClient.EXPECT().GetConnectionHandle().Return(connectionHandle, nil)
	persistentSubscriptionClientFactory.EXPECT().CreateClient(grpcClient, expectedPersistentSubscriptionsClient).
		Return(persistentSubscriptionClient)

	clientInstance := Client{
		grpcClient:              grpcClient,
		persistentClientFactory: persistentSubscriptionClientFactory,
	}

	connectionResult, err := clientInstance.ConnectToPersistentSubscription(ctx, bufferSize, groupName, streamName)
	require.Equal(t, expectedErrorResult, err)
	require.Equal(t, expectedSyncReadConnection, connectionResult)
}

func Test_Client_CreatePersistentSubscriptionToStream(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	grpcClient := connection.NewMockGrpcClient(ctrl)

	ctx := context.Background()

	streamConfig := persistent.CreateOrUpdateStreamRequest{
		StreamName: "some name",
		GroupName:  "some group",
		Revision:   persistent.StreamRevision{Revision: 10},
		Settings:   persistent.DefaultRequestSettings,
	}

	t.Run("Success", func(t *testing.T) {
		grpcClientConnection := &grpc.ClientConn{}

		persistentSubscriptionClient := persistent.NewMockClient(ctrl)
		connectionHandle := connection.NewMockConnectionHandle(ctrl)
		persistentSubscriptionClientFactory := persistent.NewMockClientFactory(ctrl)

		expectedPersistentSubscriptionsClient := persistent2.NewPersistentSubscriptionsClient(grpcClientConnection)

		connectionHandle.EXPECT().Connection().Return(grpcClientConnection)
		grpcClient.EXPECT().GetConnectionHandle().Return(connectionHandle, nil)
		persistentSubscriptionClient.EXPECT().CreateStreamSubscription(ctx, connectionHandle, streamConfig).Return(nil)
		persistentSubscriptionClientFactory.EXPECT().CreateClient(grpcClient, expectedPersistentSubscriptionsClient).
			Return(persistentSubscriptionClient)
		clientInstance := Client{
			grpcClient:              grpcClient,
			persistentClientFactory: persistentSubscriptionClientFactory,
		}

		err := clientInstance.CreatePersistentSubscription(ctx, streamConfig)
		require.NoError(t, err)
	})

	t.Run("Grpc Connection Handle Error", func(t *testing.T) {
		expectedErrorResult := errors.NewErrorCode("some error")
		grpcClient.EXPECT().GetConnectionHandle().Return(nil, expectedErrorResult)
		clientInstance := Client{
			grpcClient: grpcClient,
		}

		err := clientInstance.CreatePersistentSubscription(ctx, streamConfig)
		require.Equal(t, expectedErrorResult, err)
	})
}

func Test_Client_CreatePersistentSubscriptionToAll(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	grpcClient := connection.NewMockGrpcClient(ctrl)

	ctx := context.Background()

	streamConfig := persistent.CreateAllRequest{
		GroupName: "some group",
		Position: persistent.AllPosition{
			Commit:  10,
			Prepare: 20,
		},
		Filter: persistent.CreateRequestAllFilter{
			FilterBy:                     persistent.CreateRequestAllFilterByEventType,
			Matcher:                      persistent.CreateRequestAllFilterByRegex{Regex: "some regex"},
			Window:                       persistent.CreateRequestAllFilterWindowMax{Max: 10},
			CheckpointIntervalMultiplier: 20,
		},
		Settings: persistent.DefaultRequestSettings,
	}

	t.Run("Success", func(t *testing.T) {
		grpcClientConnection := &grpc.ClientConn{}

		persistentSubscriptionClientFactory := persistent.NewMockClientFactory(ctrl)
		connectionHandle := connection.NewMockConnectionHandle(ctrl)
		persistentSubscriptionClient := persistent.NewMockClient(ctrl)

		expectedPersistentSubscriptionsClient := persistent2.NewPersistentSubscriptionsClient(grpcClientConnection)

		connectionHandle.EXPECT().Connection().Return(grpcClientConnection)
		grpcClient.EXPECT().GetConnectionHandle().Return(connectionHandle, nil)
		persistentSubscriptionClient.EXPECT().CreateAllSubscription(ctx, connectionHandle, streamConfig).Return(nil)
		persistentSubscriptionClientFactory.EXPECT().CreateClient(grpcClient, expectedPersistentSubscriptionsClient).
			Return(persistentSubscriptionClient)

		clientInstance := Client{
			persistentClientFactory: persistentSubscriptionClientFactory,
			grpcClient:              grpcClient,
		}

		err := clientInstance.CreatePersistentSubscriptionAll(ctx, streamConfig)
		require.NoError(t, err)
	})

	t.Run("Grpc Connection Handle Error", func(t *testing.T) {
		expectedErrorResult := errors.NewErrorCode("some error")
		grpcClient.EXPECT().GetConnectionHandle().Return(nil, expectedErrorResult)

		clientInstance := Client{
			grpcClient: grpcClient,
		}

		err := clientInstance.CreatePersistentSubscriptionAll(ctx, streamConfig)
		require.Equal(t, expectedErrorResult, err)
	})
}

func Test_Client_UpdatePersistentSubscriptionToStream(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	grpcClient := connection.NewMockGrpcClient(ctrl)

	ctx := context.Background()
	streamConfig := persistent.CreateOrUpdateStreamRequest{
		StreamName: "some name",
		GroupName:  "some group",
		Revision:   persistent.StreamRevision{Revision: 10},
		Settings:   persistent.DefaultRequestSettings,
	}

	t.Run("Success", func(t *testing.T) {
		grpcClientConnection := &grpc.ClientConn{}

		persistentSubscriptionClientFactory := persistent.NewMockClientFactory(ctrl)
		connectionHandle := connection.NewMockConnectionHandle(ctrl)
		persistentSubscriptionClient := persistent.NewMockClient(ctrl)

		expectedPersistentSubscriptionsClient := persistent2.NewPersistentSubscriptionsClient(grpcClientConnection)

		connectionHandle.EXPECT().Connection().Return(grpcClientConnection)
		grpcClient.EXPECT().GetConnectionHandle().Return(connectionHandle, nil)
		persistentSubscriptionClient.EXPECT().UpdateStreamSubscription(ctx, connectionHandle, streamConfig).Return(nil)
		persistentSubscriptionClientFactory.EXPECT().CreateClient(grpcClient, expectedPersistentSubscriptionsClient).
			Return(persistentSubscriptionClient)

		clientInstance := Client{
			persistentClientFactory: persistentSubscriptionClientFactory,
			grpcClient:              grpcClient,
		}

		err := clientInstance.UpdatePersistentStreamSubscription(ctx, streamConfig)
		require.NoError(t, err)
	})

	t.Run("Grpc Connection Handle Error", func(t *testing.T) {
		expectedErrorResult := errors.NewErrorCode("some error")
		grpcClient.EXPECT().GetConnectionHandle().Return(nil, expectedErrorResult)

		clientInstance := Client{
			grpcClient: grpcClient,
		}

		err := clientInstance.UpdatePersistentStreamSubscription(ctx, streamConfig)
		require.Equal(t, expectedErrorResult, err)
	})
}

func Test_Client_UpdatePersistentSubscriptionToAll(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	grpcClient := connection.NewMockGrpcClient(ctrl)

	ctx := context.Background()
	streamConfig := persistent.UpdateAllRequest{
		GroupName: "some group",
		Position:  persistent.AllPosition{Commit: 10, Prepare: 20},
		Settings:  persistent.DefaultRequestSettings,
	}

	t.Run("Success", func(t *testing.T) {
		grpcClientConnection := &grpc.ClientConn{}

		persistentSubscriptionClientFactory := persistent.NewMockClientFactory(ctrl)
		connectionHandle := connection.NewMockConnectionHandle(ctrl)
		persistentSubscriptionClient := persistent.NewMockClient(ctrl)

		expectedPersistentSubscriptionsClient := persistent2.NewPersistentSubscriptionsClient(grpcClientConnection)

		connectionHandle.EXPECT().Connection().Return(grpcClientConnection)
		grpcClient.EXPECT().GetConnectionHandle().Return(connectionHandle, nil)
		persistentSubscriptionClient.EXPECT().UpdateAllSubscription(ctx, connectionHandle, streamConfig).Return(nil)
		persistentSubscriptionClientFactory.EXPECT().CreateClient(grpcClient, expectedPersistentSubscriptionsClient).
			Return(persistentSubscriptionClient)

		clientInstance := Client{
			persistentClientFactory: persistentSubscriptionClientFactory,
			grpcClient:              grpcClient,
		}

		err := clientInstance.UpdatePersistentSubscriptionAll(ctx, streamConfig)
		require.NoError(t, err)
	})

	t.Run("Grpc Connection Handle Error", func(t *testing.T) {
		expectedErrorResult := errors.NewErrorCode("some error")
		grpcClient.EXPECT().GetConnectionHandle().Return(nil, expectedErrorResult)

		clientInstance := Client{
			grpcClient: grpcClient,
		}

		err := clientInstance.UpdatePersistentSubscriptionAll(ctx, streamConfig)
		require.Equal(t, expectedErrorResult, err)
	})
}

func Test_Client_DeletePersistentSubscriptionToStream(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	grpcClient := connection.NewMockGrpcClient(ctrl)

	ctx := context.Background()
	streamConfig := persistent.DeleteRequest{
		StreamName: "some stream name",
		GroupName:  "some group name",
	}

	t.Run("Success", func(t *testing.T) {
		grpcClientConnection := &grpc.ClientConn{}

		persistentSubscriptionClientFactory := persistent.NewMockClientFactory(ctrl)
		connectionHandle := connection.NewMockConnectionHandle(ctrl)
		persistentSubscriptionClient := persistent.NewMockClient(ctrl)

		expectedPersistentSubscriptionsClient := persistent2.NewPersistentSubscriptionsClient(grpcClientConnection)

		connectionHandle.EXPECT().Connection().Return(grpcClientConnection)
		grpcClient.EXPECT().GetConnectionHandle().Return(connectionHandle, nil)
		persistentSubscriptionClient.EXPECT().DeleteStreamSubscription(ctx, connectionHandle, streamConfig).Return(nil)
		persistentSubscriptionClientFactory.EXPECT().CreateClient(grpcClient, expectedPersistentSubscriptionsClient).
			Return(persistentSubscriptionClient)

		clientInstance := Client{
			persistentClientFactory: persistentSubscriptionClientFactory,
			grpcClient:              grpcClient,
		}

		err := clientInstance.DeletePersistentSubscription(ctx, streamConfig)
		require.NoError(t, err)
	})

	t.Run("Grpc Connection Handle Error", func(t *testing.T) {
		expectedErrorResult := errors.NewErrorCode("some error")
		grpcClient.EXPECT().GetConnectionHandle().Return(nil, expectedErrorResult)

		clientInstance := Client{
			grpcClient: grpcClient,
		}

		err := clientInstance.DeletePersistentSubscription(ctx, streamConfig)
		require.Equal(t, expectedErrorResult, err)
	})
}

func Test_Client_DeletePersistentSubscriptionToAll(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	grpcClient := connection.NewMockGrpcClient(ctrl)

	ctx := context.Background()
	groupName := "some group name"

	t.Run("Success", func(t *testing.T) {
		grpcClientConnection := &grpc.ClientConn{}

		persistentSubscriptionClientFactory := persistent.NewMockClientFactory(ctrl)
		connectionHandle := connection.NewMockConnectionHandle(ctrl)
		persistentSubscriptionClient := persistent.NewMockClient(ctrl)

		expectedPersistentSubscriptionsClient := persistent2.NewPersistentSubscriptionsClient(grpcClientConnection)

		connectionHandle.EXPECT().Connection().Return(grpcClientConnection)
		grpcClient.EXPECT().GetConnectionHandle().Return(connectionHandle, nil)
		persistentSubscriptionClient.EXPECT().DeleteAllSubscription(ctx, connectionHandle, groupName).Return(nil)
		persistentSubscriptionClientFactory.EXPECT().CreateClient(grpcClient, expectedPersistentSubscriptionsClient).
			Return(persistentSubscriptionClient)

		clientInstance := Client{
			persistentClientFactory: persistentSubscriptionClientFactory,
			grpcClient:              grpcClient,
		}

		err := clientInstance.DeletePersistentSubscriptionAll(ctx, groupName)
		require.NoError(t, err)
	})

	t.Run("Grpc Connection Handle Error", func(t *testing.T) {
		expectedErrorResult := errors.NewErrorCode("some error")
		grpcClient.EXPECT().GetConnectionHandle().Return(nil, expectedErrorResult)

		clientInstance := Client{
			grpcClient: grpcClient,
		}

		err := clientInstance.DeletePersistentSubscriptionAll(ctx, groupName)
		require.Equal(t, expectedErrorResult, err)
	})
}
