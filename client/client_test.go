package client

import (
	"context"
	"errors"
	"testing"

	"github.com/EventStore/EventStore-Client-Go/client/filtering"
	"github.com/EventStore/EventStore-Client-Go/persistent"
	"github.com/EventStore/EventStore-Client-Go/position"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func Test_Client_ConnectToPersistentSubscription(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	persistentSubscriptionClient := persistent.NewMockClient(ctrl)

	ctx := context.Background()
	var bufferSize int32 = 10
	groupName := "some group name"
	streamName := []byte("some stream")

	expectedSyncReadConnection := persistent.NewMockSyncReadConnection(ctrl)

	expectedErrorResult := errors.New("some error")
	persistentSubscriptionClient.EXPECT().SubscribeToStreamSync(ctx, bufferSize, groupName, streamName).Return(expectedSyncReadConnection, expectedErrorResult)
	client := Client{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	connectionResult, err := client.ConnectToPersistentSubscription(ctx, bufferSize, groupName, streamName)
	require.Equal(t, expectedErrorResult, err)
	require.Equal(t, expectedSyncReadConnection, connectionResult)
}

func Test_Client_CreatePersistentSubscriptionToStream_NoError(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	persistentSubscriptionClient := persistent.NewMockClient(ctrl)

	ctx := context.Background()
	streamConfig := persistent.SubscriptionStreamConfig{
		StreamOption: persistent.StreamSettings{
			StreamName: []byte("some stream name"),
			Revision:   10,
		},
		GroupName: "some group name",
		Settings:  persistent.DefaultSubscriptionSettings,
	}

	persistentSubscriptionClient.EXPECT().CreateStreamSubscription(ctx, streamConfig).Return(nil)
	client := Client{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	err := client.CreatePersistentSubscription(ctx, streamConfig)
	require.NoError(t, err)
}

func Test_Client_CreatePersistentSubscriptionToStream_Error(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	persistentSubscriptionClient := persistent.NewMockClient(ctrl)

	ctx := context.Background()
	streamConfig := persistent.SubscriptionStreamConfig{
		StreamOption: persistent.StreamSettings{
			StreamName: []byte("some stream name"),
			Revision:   10,
		},
		GroupName: "some group name",
		Settings:  persistent.DefaultSubscriptionSettings,
	}

	expectedErrorResult := errors.New("some error")
	persistentSubscriptionClient.EXPECT().CreateStreamSubscription(ctx, streamConfig).Return(expectedErrorResult)
	client := Client{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	err := client.CreatePersistentSubscription(ctx, streamConfig)
	require.Equal(t, expectedErrorResult, err)
}

func Test_Client_CreatePersistentSubscriptionToAll_NoError(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	persistentSubscriptionClient := persistent.NewMockClient(ctrl)

	ctx := context.Background()
	streamConfig := persistent.SubscriptionAllOptionConfig{
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
				Regex:      "regex",
			},
		},
		GroupName: "some group name",
		Settings:  persistent.DefaultSubscriptionSettings,
	}

	persistentSubscriptionClient.EXPECT().CreateAllSubscription(ctx, streamConfig).Return(nil)
	client := Client{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	err := client.CreatePersistentSubscriptionAll(ctx, streamConfig)
	require.NoError(t, err)
}

func Test_Client_CreatePersistentSubscription_Error(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	persistentSubscriptionClient := persistent.NewMockClient(ctrl)

	ctx := context.Background()
	streamConfig := persistent.SubscriptionAllOptionConfig{
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
				Regex:      "regex",
			},
		},
		GroupName: "some group name",
		Settings:  persistent.DefaultSubscriptionSettings,
	}

	expectedErrorResult := errors.New("some error")
	persistentSubscriptionClient.EXPECT().CreateAllSubscription(ctx, streamConfig).Return(expectedErrorResult)
	client := Client{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	err := client.CreatePersistentSubscriptionAll(ctx, streamConfig)
	require.Equal(t, expectedErrorResult, err)
}

func Test_Client_UpdatePersistentSubscriptionToStream_NoError(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	persistentSubscriptionClient := persistent.NewMockClient(ctrl)

	ctx := context.Background()
	streamConfig := persistent.SubscriptionStreamConfig{
		StreamOption: persistent.StreamSettings{
			StreamName: []byte("stream name"),
			Revision:   10,
		},
		GroupName: "some group name",
		Settings:  persistent.DefaultSubscriptionSettings,
	}

	persistentSubscriptionClient.EXPECT().UpdateStreamSubscription(ctx, streamConfig).Return(nil)
	client := Client{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	err := client.UpdatePersistentStreamSubscription(ctx, streamConfig)
	require.NoError(t, err)
}

func Test_Client_UpdatePersistentSubscriptionToStream_Error(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	persistentSubscriptionClient := persistent.NewMockClient(ctrl)

	ctx := context.Background()
	streamConfig := persistent.SubscriptionStreamConfig{
		StreamOption: persistent.StreamSettings{
			StreamName: []byte("stream name"),
			Revision:   10,
		},
		GroupName: "some group name",
		Settings:  persistent.DefaultSubscriptionSettings,
	}

	expectedErrorResult := errors.New("some error")
	persistentSubscriptionClient.EXPECT().UpdateStreamSubscription(ctx, streamConfig).Return(expectedErrorResult)
	client := Client{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	err := client.UpdatePersistentStreamSubscription(ctx, streamConfig)
	require.Equal(t, expectedErrorResult, err)
}

func Test_Client_UpdatePersistentSubscriptionToAll_NoError(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	persistentSubscriptionClient := persistent.NewMockClient(ctrl)

	ctx := context.Background()
	streamConfig := persistent.SubscriptionUpdateAllOptionConfig{
		Position: position.Position{
			Commit:  10,
			Prepare: 20,
		},
		GroupName: "some group name",
		Settings:  persistent.DefaultSubscriptionSettings,
	}

	persistentSubscriptionClient.EXPECT().UpdateAllSubscription(ctx, streamConfig).Return(nil)
	client := Client{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	err := client.UpdatePersistentSubscriptionAll(ctx, streamConfig)
	require.NoError(t, err)
}

func Test_Client_UpdatePersistentSubscriptionToAll_Error(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	persistentSubscriptionClient := persistent.NewMockClient(ctrl)

	ctx := context.Background()
	streamConfig := persistent.SubscriptionUpdateAllOptionConfig{
		Position: position.Position{
			Commit:  10,
			Prepare: 20,
		},
		GroupName: "some group name",
		Settings:  persistent.DefaultSubscriptionSettings,
	}

	expectedErrorResult := errors.New("some error")
	persistentSubscriptionClient.EXPECT().UpdateAllSubscription(ctx, streamConfig).Return(expectedErrorResult)
	client := Client{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	err := client.UpdatePersistentSubscriptionAll(ctx, streamConfig)
	require.Equal(t, expectedErrorResult, err)
}

func Test_Client_DeletePersistentSubscriptionToStream_NoError(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	persistentSubscriptionClient := persistent.NewMockClient(ctrl)

	ctx := context.Background()
	streamConfig := persistent.DeleteOptions{
		GroupName:  "some group name",
		StreamName: []byte("some stream name"),
	}

	persistentSubscriptionClient.EXPECT().DeleteStreamSubscription(ctx, streamConfig).Return(nil)
	client := Client{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	err := client.DeletePersistentSubscription(ctx, streamConfig)
	require.NoError(t, err)
}

func Test_Client_DeletePersistentSubscriptionToStream_Error(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	persistentSubscriptionClient := persistent.NewMockClient(ctrl)

	ctx := context.Background()
	streamConfig := persistent.DeleteOptions{
		GroupName:  "some group name",
		StreamName: []byte("some stream name"),
	}

	expectedErrorResult := errors.New("some error")
	persistentSubscriptionClient.EXPECT().DeleteStreamSubscription(ctx, streamConfig).Return(expectedErrorResult)
	client := Client{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	err := client.DeletePersistentSubscription(ctx, streamConfig)
	require.Equal(t, expectedErrorResult, err)
}

func Test_Client_DeletePersistentSubscriptionToAll_NoError(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	persistentSubscriptionClient := persistent.NewMockClient(ctrl)

	ctx := context.Background()
	groupName := "some group name"

	persistentSubscriptionClient.EXPECT().DeleteAllSubscription(ctx, groupName).Return(nil)
	client := Client{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	err := client.DeletePersistentSubscriptionAll(ctx, groupName)
	require.NoError(t, err)
}

func Test_Client_DeletePersistentSubscriptionToAll_Error(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	persistentSubscriptionClient := persistent.NewMockClient(ctrl)

	ctx := context.Background()
	groupName := "some group name"

	expectedErrorResult := errors.New("some error")
	persistentSubscriptionClient.EXPECT().DeleteAllSubscription(ctx, groupName).Return(expectedErrorResult)
	client := Client{
		persistentSubscriptionClient: persistentSubscriptionClient,
	}

	err := client.DeletePersistentSubscriptionAll(ctx, groupName)
	require.Equal(t, expectedErrorResult, err)
}
