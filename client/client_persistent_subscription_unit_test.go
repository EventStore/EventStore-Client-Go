package client_test

import (
	"context"
	"errors"
	"testing"

	"github.com/EventStore/EventStore-Client-Go/client/filtering"
	"github.com/EventStore/EventStore-Client-Go/position"

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
	streamName := []byte("some stream")

	grpcClientConnection := &grpc.ClientConn{}
	expectedErrorResult := errors.New("some error")
	expectedPersistentSubscriptionsClient := persistent2.NewPersistentSubscriptionsClient(grpcClientConnection)

	persistentSubscriptionClient.EXPECT().
		SubscribeToStreamSync(ctx, connectionHandle, bufferSize, groupName, streamName).
		Return(expectedSyncReadConnection, expectedErrorResult)
	connectionHandle.EXPECT().Connection().Return(grpcClientConnection)
	grpcClient.EXPECT().GetConnectionHandle().Return(connectionHandle, nil)
	persistentSubscriptionClientFactory.EXPECT().CreateClient(grpcClient, expectedPersistentSubscriptionsClient).
		Return(persistentSubscriptionClient)

	client := Client{
		grpcClient:              grpcClient,
		persistentClientFactory: persistentSubscriptionClientFactory,
	}

	connectionResult, err := client.ConnectToPersistentSubscription(ctx, bufferSize, groupName, streamName)
	require.Equal(t, expectedErrorResult, err)
	require.Equal(t, expectedSyncReadConnection, connectionResult)
}

func Test_Client_CreatePersistentSubscriptionToStream(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	grpcClient := connection.NewMockGrpcClient(ctrl)

	ctx := context.Background()
	streamConfig := persistent.SubscriptionStreamConfig{
		StreamOption: persistent.StreamSettings{
			StreamName: []byte("some stream name"),
			Revision:   10,
		},
		GroupName: "some group name",
		Settings:  persistent.DefaultSubscriptionSettings,
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
		client := Client{
			grpcClient:              grpcClient,
			persistentClientFactory: persistentSubscriptionClientFactory,
		}

		err := client.CreatePersistentSubscription(ctx, streamConfig)
		require.NoError(t, err)
	})

	t.Run("Grpc Connection Handle Error", func(t *testing.T) {
		expectedErrorResult := errors.New("some error")
		grpcClient.EXPECT().GetConnectionHandle().Return(nil, expectedErrorResult)
		client := Client{
			grpcClient: grpcClient,
		}

		err := client.CreatePersistentSubscription(ctx, streamConfig)
		require.Equal(t, expectedErrorResult, err)
	})
}

func Test_Client_CreatePersistentSubscriptionToAll(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	grpcClient := connection.NewMockGrpcClient(ctrl)

	ctx := context.Background()
	streamConfig := persistent.SubscriptionAllOptionConfig{
		Position: position.Position{
			Commit:  10,
			Prepare: 20,
		},
		Filter: &filtering.SubscriptionFilterOptions{
			MaxSearchWindow:    10,
			CheckpointInterval: 20,
			SubscriptionFilter: filtering.SubscriptionFilter{
				FilterType: filtering.EventFilter,
				Prefixes:   nil,
				Regex:      "regex",
			},
		},
		GroupName: "some group name",
		Settings:  persistent.DefaultSubscriptionSettings,
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

		client := Client{
			persistentClientFactory: persistentSubscriptionClientFactory,
			grpcClient:              grpcClient,
		}

		err := client.CreatePersistentSubscriptionAll(ctx, streamConfig)
		require.NoError(t, err)
	})

	t.Run("Grpc Connection Handle Error", func(t *testing.T) {
		expectedErrorResult := errors.New("some error")
		grpcClient.EXPECT().GetConnectionHandle().Return(nil, expectedErrorResult)

		client := Client{
			grpcClient: grpcClient,
		}

		err := client.CreatePersistentSubscriptionAll(ctx, streamConfig)
		require.Equal(t, expectedErrorResult, err)
	})
}

func Test_Client_UpdatePersistentSubscriptionToStream(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	grpcClient := connection.NewMockGrpcClient(ctrl)

	ctx := context.Background()
	streamConfig := persistent.SubscriptionStreamConfig{
		StreamOption: persistent.StreamSettings{
			StreamName: []byte("stream name"),
			Revision:   10,
		},
		GroupName: "some group name",
		Settings:  persistent.DefaultSubscriptionSettings,
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

		client := Client{
			persistentClientFactory: persistentSubscriptionClientFactory,
			grpcClient:              grpcClient,
		}

		err := client.UpdatePersistentStreamSubscription(ctx, streamConfig)
		require.NoError(t, err)
	})

	t.Run("Grpc Connection Handle Error", func(t *testing.T) {
		expectedErrorResult := errors.New("some error")
		grpcClient.EXPECT().GetConnectionHandle().Return(nil, expectedErrorResult)

		client := Client{
			grpcClient: grpcClient,
		}

		err := client.UpdatePersistentStreamSubscription(ctx, streamConfig)
		require.Equal(t, expectedErrorResult, err)
	})
}

func Test_Client_UpdatePersistentSubscriptionToAll(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	grpcClient := connection.NewMockGrpcClient(ctrl)

	ctx := context.Background()
	streamConfig := persistent.SubscriptionUpdateAllOptionConfig{
		Position: position.Position{
			Commit:  10,
			Prepare: 20,
		},
		GroupName: "some group name",
		Settings:  persistent.DefaultSubscriptionSettings,
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

		client := Client{
			persistentClientFactory: persistentSubscriptionClientFactory,
			grpcClient:              grpcClient,
		}

		err := client.UpdatePersistentSubscriptionAll(ctx, streamConfig)
		require.NoError(t, err)
	})

	t.Run("Grpc Connection Handle Error", func(t *testing.T) {
		expectedErrorResult := errors.New("some error")
		grpcClient.EXPECT().GetConnectionHandle().Return(nil, expectedErrorResult)

		client := Client{
			grpcClient: grpcClient,
		}

		err := client.UpdatePersistentSubscriptionAll(ctx, streamConfig)
		require.Equal(t, expectedErrorResult, err)
	})
}

func Test_Client_DeletePersistentSubscriptionToStream(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	grpcClient := connection.NewMockGrpcClient(ctrl)

	ctx := context.Background()
	streamConfig := persistent.DeleteOptions{
		GroupName:  "some group name",
		StreamName: []byte("some stream name"),
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

		client := Client{
			persistentClientFactory: persistentSubscriptionClientFactory,
			grpcClient:              grpcClient,
		}

		err := client.DeletePersistentSubscription(ctx, streamConfig)
		require.NoError(t, err)
	})

	t.Run("Grpc Connection Handle Error", func(t *testing.T) {
		expectedErrorResult := errors.New("some error")
		grpcClient.EXPECT().GetConnectionHandle().Return(nil, expectedErrorResult)

		client := Client{
			grpcClient: grpcClient,
		}

		err := client.DeletePersistentSubscription(ctx, streamConfig)
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

		client := Client{
			persistentClientFactory: persistentSubscriptionClientFactory,
			grpcClient:              grpcClient,
		}

		err := client.DeletePersistentSubscriptionAll(ctx, groupName)
		require.NoError(t, err)
	})

	t.Run("Grpc Connection Handle Error", func(t *testing.T) {
		expectedErrorResult := errors.New("some error")
		grpcClient.EXPECT().GetConnectionHandle().Return(nil, expectedErrorResult)

		client := Client{
			grpcClient: grpcClient,
		}

		err := client.DeletePersistentSubscriptionAll(ctx, groupName)
		require.Equal(t, expectedErrorResult, err)
	})
}
