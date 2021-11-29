package esdb_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/stretchr/testify/require"
)

func PersistentSubTests(t *testing.T, emptyDBClient *esdb.Client, populatedDBClient *esdb.Client) {
	t.Run("PersistentSubTests", func(t *testing.T) {
		t.Run("createPersistentStreamSubscription", createPersistentStreamSubscription(emptyDBClient))
		t.Run("createPersistentStreamSubscription_MessageTimeoutZero", createPersistentStreamSubscription_MessageTimeoutZero(emptyDBClient))
		t.Run("createPersistentStreamSubscription_StreamNotExits", createPersistentStreamSubscription_StreamNotExits(emptyDBClient))
		t.Run("createPersistentStreamSubscription_FailsIfAlreadyExists", createPersistentStreamSubscription_FailsIfAlreadyExists(emptyDBClient))
		t.Run("createPersistentStreamSubscription_AfterDeleting", createPersistentStreamSubscription_AfterDeleting(emptyDBClient))
		t.Run("updatePersistentStreamSubscription", updatePersistentStreamSubscription(emptyDBClient))
		t.Run("updatePersistentStreamSubscription_ErrIfSubscriptionDoesNotExist", updatePersistentStreamSubscription_ErrIfSubscriptionDoesNotExist(emptyDBClient))
		t.Run("deletePersistentStreamSubscription", deletePersistentStreamSubscription(emptyDBClient))
		t.Run("deletePersistentSubscription_ErrIfSubscriptionDoesNotExist", deletePersistentSubscription_ErrIfSubscriptionDoesNotExist(emptyDBClient))
		t.Run("testPersistentSubscriptionClosing", testPersistentSubscriptionClosing(populatedDBClient))
		t.Run("persistentAllCreate", persistentAllCreate(emptyDBClient))
		t.Run("persistentAllUpdate", persistentAllUpdate(emptyDBClient))
		t.Run("persistentAllDelete", persistentAllDelete(emptyDBClient))
	})
}

func createPersistentStreamSubscription(clientInstance *esdb.Client) TestCall {
	return func(t *testing.T) {
		streamID := NAME_GENERATOR.Generate()
		pushEventToStream(t, clientInstance, streamID)

		err := clientInstance.CreatePersistentSubscription(
			context.Background(),
			streamID,
			"Group 1",
			esdb.PersistentStreamSubscriptionOptions{},
		)

		require.NoError(t, err)
	}
}

func createPersistentStreamSubscription_MessageTimeoutZero(clientInstance *esdb.Client) TestCall {
	return func(t *testing.T) {
		streamID := NAME_GENERATOR.Generate()
		pushEventToStream(t, clientInstance, streamID)

		settings := esdb.SubscriptionSettingsDefault()
		settings.MessageTimeoutInMs = 0

		err := clientInstance.CreatePersistentSubscription(
			context.Background(),
			streamID,
			"Group 1",
			esdb.PersistentStreamSubscriptionOptions{
				Settings: &settings,
			},
		)

		require.NoError(t, err)
	}
}

func createPersistentStreamSubscription_StreamNotExits(clientInstance *esdb.Client) TestCall {
	return func(t *testing.T) {
		streamID := NAME_GENERATOR.Generate()

		err := clientInstance.CreatePersistentSubscription(
			context.Background(),
			streamID,
			"Group 1",
			esdb.PersistentStreamSubscriptionOptions{},
		)

		require.NoError(t, err)
	}
}

func createPersistentStreamSubscription_FailsIfAlreadyExists(clientInstance *esdb.Client) TestCall {
	return func(t *testing.T) {
		streamID := NAME_GENERATOR.Generate()
		pushEventToStream(t, clientInstance, streamID)

		err := clientInstance.CreatePersistentSubscription(
			context.Background(),
			streamID,
			"Group 1",
			esdb.PersistentStreamSubscriptionOptions{},
		)

		require.NoError(t, err)

		err = clientInstance.CreatePersistentSubscription(
			context.Background(),
			streamID,
			"Group 1",
			esdb.PersistentStreamSubscriptionOptions{},
		)

		require.Error(t, err)
	}
}

func createPersistentStreamSubscription_AfterDeleting(clientInstance *esdb.Client) TestCall {
	return func(t *testing.T) {
		streamID := NAME_GENERATOR.Generate()
		pushEventToStream(t, clientInstance, streamID)

		err := clientInstance.CreatePersistentSubscription(
			context.Background(),
			streamID,
			"Group 1",
			esdb.PersistentStreamSubscriptionOptions{},
		)

		require.NoError(t, err)

		err = clientInstance.DeletePersistentSubscription(context.Background(), streamID, "Group 1", esdb.DeletePersistentSubscriptionOptions{})

		require.NoError(t, err)

		err = clientInstance.CreatePersistentSubscription(
			context.Background(),
			streamID,
			"Group 1",
			esdb.PersistentStreamSubscriptionOptions{},
		)

		require.NoError(t, err)
	}
}

func updatePersistentStreamSubscription(clientInstance *esdb.Client) TestCall {
	return func(t *testing.T) {
		streamID := NAME_GENERATOR.Generate()
		pushEventToStream(t, clientInstance, streamID)

		err := clientInstance.CreatePersistentSubscription(
			context.Background(),
			streamID,
			"Group 1",
			esdb.PersistentStreamSubscriptionOptions{},
		)

		require.NoError(t, err)

		settings := esdb.SubscriptionSettingsDefault()
		settings.HistoryBufferSize = settings.HistoryBufferSize + 1
		settings.NamedConsumerStrategy = esdb.ConsumerStrategy_DispatchToSingle
		settings.MaxSubscriberCount = settings.MaxSubscriberCount + 1
		settings.ReadBatchSize = settings.ReadBatchSize + 1
		settings.CheckpointAfterInMs = settings.CheckpointAfterInMs + 1
		settings.MaxCheckpointCount = settings.MaxCheckpointCount + 1
		settings.MinCheckpointCount = settings.MinCheckpointCount + 1
		settings.LiveBufferSize = settings.LiveBufferSize + 1
		settings.MaxRetryCount = settings.MaxRetryCount + 1
		settings.MessageTimeoutInMs = settings.MessageTimeoutInMs + 1
		settings.ExtraStatistics = !settings.ExtraStatistics
		settings.ResolveLinkTos = !settings.ResolveLinkTos

		err = clientInstance.UpdatePersistentStreamSubscription(context.Background(), streamID, "Group 1", esdb.PersistentStreamSubscriptionOptions{
			Settings: &settings,
		})

		require.NoError(t, err)
	}
}

func updatePersistentStreamSubscription_ErrIfSubscriptionDoesNotExist(clientInstance *esdb.Client) TestCall {
	return func(t *testing.T) {
		streamID := NAME_GENERATOR.Generate()

		err := clientInstance.UpdatePersistentStreamSubscription(context.Background(), streamID, "Group 1", esdb.PersistentStreamSubscriptionOptions{})

		require.Error(t, err)
	}
}

func deletePersistentStreamSubscription(clientInstance *esdb.Client) TestCall {
	return func(t *testing.T) {
		streamID := NAME_GENERATOR.Generate()
		pushEventToStream(t, clientInstance, streamID)

		err := clientInstance.CreatePersistentSubscription(
			context.Background(),
			streamID,
			"Group 1",
			esdb.PersistentStreamSubscriptionOptions{},
		)

		require.NoError(t, err)

		err = clientInstance.DeletePersistentSubscription(
			context.Background(),
			streamID,
			"Group 1",
			esdb.DeletePersistentSubscriptionOptions{},
		)

		require.NoError(t, err)
	}
}

func deletePersistentSubscription_ErrIfSubscriptionDoesNotExist(clientInstance *esdb.Client) TestCall {
	return func(t *testing.T) {
		err := clientInstance.DeletePersistentSubscription(
			context.Background(),
			NAME_GENERATOR.Generate(),
			"a",
			esdb.DeletePersistentSubscriptionOptions{},
		)

		require.Error(t, err)
	}
}

func pushEventToStream(t *testing.T, clientInstance *esdb.Client, streamID string) {
	testEvent := createTestEvent()
	pushEventsToStream(t, clientInstance, streamID, []esdb.EventData{testEvent})
}

func pushEventsToStream(t *testing.T,
	clientInstance *esdb.Client,
	streamID string,
	events []esdb.EventData) {

	opts := esdb.AppendToStreamOptions{
		ExpectedRevision: esdb.NoStream{},
	}
	_, err := clientInstance.AppendToStream(context.Background(), streamID, opts, events...)

	require.NoError(t, err)
}

func testPersistentSubscriptionClosing(db *esdb.Client) TestCall {
	return func(t *testing.T) {
		if db == nil {
			t.Skip()
		}

		streamID := "dataset20M-0"
		groupName := "Group 1"

		err := db.CreatePersistentSubscription(context.Background(), streamID, groupName, esdb.PersistentStreamSubscriptionOptions{
			From: esdb.Start{},
		})

		require.NoError(t, err)

		var receivedEvents sync.WaitGroup
		var droppedEvent sync.WaitGroup

		subscription, err := db.ConnectToPersistentSubscription(
			context.Background(), streamID, groupName, esdb.ConnectToPersistentSubscriptionOptions{
				BatchSize: 2,
			})

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

				if subEvent.SubscriptionDropped != nil {
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
}

func persistentAllCreate(client *esdb.Client) TestCall {
	return func(t *testing.T) {
		groupName := NAME_GENERATOR.Generate()

		err := client.CreatePersistentSubscriptionAll(
			context.Background(),
			groupName,
			esdb.PersistentAllSubscriptionOptions{},
		)

		if err != nil && IsESDB_VersionBelow_21() {
			t.Skip()
		}

		require.NoError(t, err)
	}
}

func persistentAllUpdate(client *esdb.Client) TestCall {
	return func(t *testing.T) {
		groupName := NAME_GENERATOR.Generate()

		err := client.CreatePersistentSubscriptionAll(
			context.Background(),
			groupName,
			esdb.PersistentAllSubscriptionOptions{},
		)

		if err != nil && IsESDB_VersionBelow_21() {
			t.Skip()
		}

		require.NoError(t, err)

		setts := esdb.SubscriptionSettingsDefault()
		setts.ResolveLinkTos = true

		err = client.UpdatePersistentSubscriptionAll(context.Background(), groupName, esdb.PersistentAllSubscriptionOptions{
			Settings: &setts,
		})

		require.NoError(t, err)
	}
}

func persistentAllDelete(client *esdb.Client) TestCall {
	return func(t *testing.T) {
		groupName := NAME_GENERATOR.Generate()

		err := client.CreatePersistentSubscriptionAll(
			context.Background(),
			groupName,
			esdb.PersistentAllSubscriptionOptions{},
		)

		if err != nil && IsESDB_VersionBelow_21() {
			t.Skip()
		}

		require.NoError(t, err)

		err = client.DeletePersistentSubscriptionAll(context.Background(), groupName, esdb.DeletePersistentSubscriptionOptions{})

		require.NoError(t, err)
	}
}
