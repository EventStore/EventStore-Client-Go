package client_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/client"
	"github.com/EventStore/EventStore-Client-Go/messages"
	"github.com/EventStore/EventStore-Client-Go/persistent"
	stream_revision "github.com/EventStore/EventStore-Client-Go/streamrevision"
	"github.com/stretchr/testify/require"
)

func Test_CreatePersistentStreamSubscription(t *testing.T) {
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClient(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	streamID := "someStream"
	pushEventToStream(t, clientInstance, streamID)

	err := clientInstance.CreatePersistentSubscription(
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

func Test_CreatePersistentStreamSubscription_MessageTimeoutZero(t *testing.T) {
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClient(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	streamID := "someStream"
	pushEventToStream(t, clientInstance, streamID)

	settings := persistent.DefaultSubscriptionSettings
	settings.MessageTimeoutInMs = 0

	err := clientInstance.CreatePersistentSubscription(
		context.Background(),
		persistent.SubscriptionStreamConfig{
			StreamOption: persistent.StreamSettings{
				StreamName: []byte(streamID),
				Revision:   persistent.Revision_Start,
			},
			GroupName: "Group 1",
			Settings:  settings,
		},
	)

	require.NoError(t, err)
}

func Test_CreatePersistentStreamSubscription_StreamNotExits(t *testing.T) {
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClient(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	streamID := "someStream"

	err := clientInstance.CreatePersistentSubscription(
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

func Test_CreatePersistentStreamSubscription_FailsIfAlreadyExists(t *testing.T) {
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClient(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	streamID := "someStream"
	pushEventToStream(t, clientInstance, streamID)

	err := clientInstance.CreatePersistentSubscription(
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

	require.Error(t, err)
}

func Test_CreatePersistentStreamSubscription_AfterDeleting(t *testing.T) {
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClient(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	streamID := "someStream"
	pushEventToStream(t, clientInstance, streamID)

	streamConfig := persistent.SubscriptionStreamConfig{
		StreamOption: persistent.StreamSettings{
			StreamName: []byte(streamID),
			Revision:   persistent.Revision_Start,
		},
		GroupName: "Group 1",
		Settings:  persistent.DefaultSubscriptionSettings,
	}

	err := clientInstance.CreatePersistentSubscription(context.Background(), streamConfig)

	require.NoError(t, err)

	err = clientInstance.DeletePersistentSubscription(context.Background(),
		persistent.DeleteOptions{
			StreamName: streamConfig.StreamOption.StreamName,
			GroupName:  streamConfig.GroupName,
		})

	require.NoError(t, err)

	err = clientInstance.CreatePersistentSubscription(context.Background(), streamConfig)

	require.NoError(t, err)
}

func Test_UpdatePersistentStreamSubscription(t *testing.T) {
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClient(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	streamID := "someStream"
	pushEventToStream(t, clientInstance, streamID)

	streamConfig := persistent.SubscriptionStreamConfig{
		StreamOption: persistent.StreamSettings{
			StreamName: []byte(streamID),
			Revision:   persistent.Revision_Start,
		},
		GroupName: "Group 1",
		Settings:  persistent.DefaultSubscriptionSettings,
	}

	err := clientInstance.CreatePersistentSubscription(context.Background(), streamConfig)

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

	err = clientInstance.UpdatePersistentStreamSubscription(context.Background(), streamConfig)

	require.NoError(t, err)
}

func Test_UpdatePersistentStreamSubscription_ErrIfSubscriptionDoesNotExist(t *testing.T) {
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClient(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	streamID := "someStream"

	streamConfig := persistent.SubscriptionStreamConfig{
		StreamOption: persistent.StreamSettings{
			StreamName: []byte(streamID),
			Revision:   persistent.Revision_Start,
		},
		GroupName: "Group 1",
		Settings:  persistent.DefaultSubscriptionSettings,
	}

	err := clientInstance.UpdatePersistentStreamSubscription(context.Background(), streamConfig)

	require.Error(t, err)
}

func Test_DeletePersistentStreamSubscription(t *testing.T) {
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClient(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	streamID := "someStream"
	pushEventToStream(t, clientInstance, streamID)

	streamConfig := persistent.SubscriptionStreamConfig{
		StreamOption: persistent.StreamSettings{
			StreamName: []byte(streamID),
			Revision:   persistent.Revision_Start,
		},
		GroupName: "Group 1",
		Settings:  persistent.DefaultSubscriptionSettings,
	}

	err := clientInstance.CreatePersistentSubscription(context.Background(), streamConfig)

	require.NoError(t, err)

	err = clientInstance.DeletePersistentSubscription(context.Background(),
		persistent.DeleteOptions{
			StreamName: streamConfig.StreamOption.StreamName,
			GroupName:  streamConfig.GroupName,
		})

	require.NoError(t, err)
}

func Test_DeletePersistentSubscription_ErrIfSubscriptionDoesNotExist(t *testing.T) {
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClient(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	err := clientInstance.DeletePersistentSubscription(context.Background(),
		persistent.DeleteOptions{
			StreamName: []byte("a"),
			GroupName:  "a",
		})

	require.Error(t, err)
}

type CloseClientInstance func()

func initializeContainerAndClient(t *testing.T,
	environmentVariables ...string) (*Container, *client.Client, CloseClientInstance) {
	container := GetEmptyDatabase(environmentVariables...)
	clientInstance := CreateTestClient(container, t)
	closeClientInstance := func() {
		err := clientInstance.Close()
		require.NoError(t, err)
	}
	return container, clientInstance, closeClientInstance
}

func pushEventToStream(t *testing.T, clientInstance *client.Client, streamID string) {
	testEvent := createTestEvent()
	pushEventsToStream(t, clientInstance, streamID, []messages.ProposedEvent{testEvent})
}

func pushEventsToStream(t *testing.T,
	clientInstance *client.Client,
	streamID string,
	events []messages.ProposedEvent) {
	_, err := clientInstance.AppendToStream(
		context.Background(),
		streamID,
		stream_revision.StreamRevisionNoStream,
		events)

	require.NoError(t, err)
}

func TestPersistentSubscriptionClosing(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()
	client := CreateTestClient(container, t)
	defer client.Close()

	streamID := "dataset20M-0"
	groupName := "Group 1"
	var bufferSize int32 = 2

	streamConfig := persistent.SubscriptionStreamConfig{
		StreamOption: persistent.StreamSettings{
			StreamName: []byte(streamID),
			Revision:   persistent.Revision_Start,
		},
		GroupName: groupName,
		Settings:  persistent.DefaultSubscriptionSettings,
	}

	err := client.CreatePersistentSubscription(context.Background(), streamConfig)

	require.NoError(t, err)

	var receivedEvents sync.WaitGroup
	var droppedEvent sync.WaitGroup

	subscription, err := client.ConnectToPersistentSubscription(
		context.Background(), bufferSize, groupName, []byte(streamID))

	require.NoError(t, err)

	go func() {
		current := 1

		for {
			subEvent := subscription.Recv()

			if subEvent.EventAppeared != nil {
				if current <= 10 {
					receivedEvents.Done()
					current++
				}

				subscription.Ack(subEvent.EventAppeared)

				continue
			}

			if subEvent.Dropped != nil {
				droppedEvent.Done()
				break
			}
		}
	}()

	require.NoError(t, err)
	receivedEvents.Add(10)
	droppedEvent.Add(1)
	timedOut := waitWithTimeout(&receivedEvents, time.Duration(5)*time.Second)
	require.False(t, timedOut, "Timed out waiting for initial set of events")
	subscription.Close()
	timedOut = waitWithTimeout(&droppedEvent, time.Duration(5)*time.Second)
	require.False(t, timedOut, "Timed out waiting for dropped event")
}
