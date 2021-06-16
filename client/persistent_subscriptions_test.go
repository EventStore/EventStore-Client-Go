package client_test

import (
	"context"
	"testing"

	"github.com/EventStore/EventStore-Client-Go/client"
	"github.com/EventStore/EventStore-Client-Go/messages"
	"github.com/EventStore/EventStore-Client-Go/persistent"
	stream_revision "github.com/EventStore/EventStore-Client-Go/streamrevision"
	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/require"
)

func Test_CreatePersistentStreamSubscription(t *testing.T) {
	containerInstance, clientInstance := initializeContainerAndClient(t)
	defer func() {
		err := clientInstance.Close()
		require.NoError(t, err)
	}()
	defer containerInstance.Close()

	testEvent := messages.ProposedEvent{
		EventID:      uuid.FromStringOrNil("38fffbc2-339e-11ea-8c7b-784f43837872"),
		EventType:    "TestEvent",
		ContentType:  "application/octet-stream",
		UserMetadata: []byte{0xd, 0xe, 0xa, 0xd},
		Data:         []byte{0xb, 0xe, 0xe, 0xf},
	}
	proposedEvents := []messages.ProposedEvent{
		testEvent,
	}
	streamID := "someStream"
	_, err := clientInstance.AppendToStream(
		context.Background(),
		streamID,
		stream_revision.StreamRevisionNoStream,
		proposedEvents)

	require.NoError(t, err)

	err = clientInstance.CreatePersistentSubscription(
		context.Background(),
		persistent.SubscriptionStreamConfig{
			StreamOption: persistent.StreamSettings{
				StreamName: []byte(streamID),
				Revision:   persistent.Revision_Start,
			},
			GroupName: "Group 1",
			Settings:  persistent.DefaultSubscriptionSettings,
		},
	)

	require.NoError(t, err)
}

func Test_CreatePersistentSubscriptionAll(t *testing.T) {
}

func Test_UpdatePersistentStreamSubscription(t *testing.T) {
	containerInstance, clientInstance := initializeContainerAndClient(t)
	defer func() {
		err := clientInstance.Close()
		require.NoError(t, err)
	}()
	defer containerInstance.Close()

	testEvent := messages.ProposedEvent{
		EventID:      uuid.FromStringOrNil("38fffbc2-339e-11ea-8c7b-784f43837872"),
		EventType:    "TestEvent",
		ContentType:  "application/octet-stream",
		UserMetadata: []byte{0xd, 0xe, 0xa, 0xd},
		Data:         []byte{0xb, 0xe, 0xe, 0xf},
	}
	proposedEvents := []messages.ProposedEvent{
		testEvent,
	}
	streamID := "someStream"
	_, err := clientInstance.AppendToStream(
		context.Background(),
		streamID,
		stream_revision.StreamRevisionNoStream,
		proposedEvents)

	require.NoError(t, err)

	streamConfig := persistent.SubscriptionStreamConfig{
		StreamOption: persistent.StreamSettings{
			StreamName: []byte(streamID),
			Revision:   persistent.Revision_Start,
		},
		GroupName: "Group 1",
		Settings:  persistent.DefaultSubscriptionSettings,
	}

	err = clientInstance.CreatePersistentSubscription(context.Background(), streamConfig)

	require.NoError(t, err)

	streamConfig.Settings.HistoryBufferSize = streamConfig.Settings.HistoryBufferSize + 1
	streamConfig.Settings.NamedConsumerStrategy = persistent.ConsumerStrategy_DispatchToSingle
	streamConfig.Settings.MaxSubscriberCount = streamConfig.Settings.MaxSubscriberCount + 1
	streamConfig.Settings.ReadBatchSize = streamConfig.Settings.ReadBatchSize + 1
	streamConfig.Settings.CheckpointAfterInMs = streamConfig.Settings.CheckpointAfterInMs + 1
	streamConfig.Settings.MaxCheckpointCount = streamConfig.Settings.MaxCheckpointCount + 1
	streamConfig.Settings.MinCheckpointCount = streamConfig.Settings.MinCheckpointCount + 1
	streamConfig.Settings.LiveBufferSize = streamConfig.Settings.LiveBufferSize + 1
	streamConfig.Settings.MaxRetryCount = streamConfig.Settings.MaxRetryCount + 1
	streamConfig.Settings.MessageTimeoutInMs = streamConfig.Settings.MessageTimeoutInMs + 1
	streamConfig.Settings.ExtraStatistics = !streamConfig.Settings.ExtraStatistics
	streamConfig.Settings.ResolveLinks = !streamConfig.Settings.ResolveLinks

	err = clientInstance.UpdatePersistentSubscription(context.Background(), streamConfig)

	require.NoError(t, err)
}

func Test_UpdatePersistentSubscriptionAll(t *testing.T) {
}

func Test_DeletePersistentStreamSubscription(t *testing.T) {
	containerInstance, clientInstance := initializeContainerAndClient(t)
	defer func() {
		err := clientInstance.Close()
		require.NoError(t, err)
	}()
	defer containerInstance.Close()

	testEvent := messages.ProposedEvent{
		EventID:      uuid.FromStringOrNil("38fffbc2-339e-11ea-8c7b-784f43837872"),
		EventType:    "TestEvent",
		ContentType:  "application/octet-stream",
		UserMetadata: []byte{0xd, 0xe, 0xa, 0xd},
		Data:         []byte{0xb, 0xe, 0xe, 0xf},
	}
	proposedEvents := []messages.ProposedEvent{
		testEvent,
	}
	streamID := "someStream"
	_, err := clientInstance.AppendToStream(
		context.Background(),
		streamID,
		stream_revision.StreamRevisionNoStream,
		proposedEvents)

	require.NoError(t, err)

	streamConfig := persistent.SubscriptionStreamConfig{
		StreamOption: persistent.StreamSettings{
			StreamName: []byte(streamID),
			Revision:   persistent.Revision_Start,
		},
		GroupName: "Group 1",
		Settings:  persistent.DefaultSubscriptionSettings,
	}

	err = clientInstance.CreatePersistentSubscription(context.Background(), streamConfig)

	require.NoError(t, err)

	err = clientInstance.DeletePersistentSubscription(context.Background(),
		persistent.DeleteOptions{
			StreamName: streamConfig.StreamOption.StreamName,
			GroupName:  streamConfig.GroupName,
		})

	require.NoError(t, err)
}

func Test_DeletePersistentSubscription_ErrIfSubscriptionDoesNotExist(t *testing.T) {
	containerInstance, clientInstance := initializeContainerAndClient(t)
	defer func() {
		err := clientInstance.Close()
		require.NoError(t, err)
	}()
	defer containerInstance.Close()

	err := clientInstance.DeletePersistentSubscription(context.Background(),
		persistent.DeleteOptions{
			StreamName: []byte("a"),
			GroupName:  "a",
		})

	require.Error(t, err)
}

func Test_DeletePersistentSubscriptionAll(t *testing.T) {
}

func initializeContainerAndClient(t *testing.T) (*Container, *client.Client) {
	container := GetEmptyDatabase()
	clientInstance := CreateTestClient(container, t)
	err := clientInstance.Connect()
	require.NoError(t, err)
	return container, clientInstance
}
