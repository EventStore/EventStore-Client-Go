package esdb_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/stretchr/testify/assert"
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
		t.Run("persistentListAllSubs", persistentListAllSubs(emptyDBClient))
		t.Run("persistentReplayParkedMessages", persistentReplayParkedMessages(emptyDBClient))
		t.Run("persistentListSubsForStream", persistentListSubsForStream(emptyDBClient))
		t.Run("persistentGetInfo", persistentGetInfo(emptyDBClient))
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
		settings.MessageTimeout = 0

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

		esdbErr, ok := esdb.FromError(err)
		assert.False(t, ok)
		assert.Equal(t, esdbErr.Code(), esdb.ErrorResourceAlreadyExists)
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
		settings.ConsumerStrategyName = esdb.ConsumerStrategy_DispatchToSingle
		settings.MaxSubscriberCount = settings.MaxSubscriberCount + 1
		settings.ReadBatchSize = settings.ReadBatchSize + 1
		settings.CheckpointAfter = settings.CheckpointAfter + 1
		settings.CheckpointUpperBound = settings.CheckpointUpperBound + 1
		settings.CheckpointLowerBound = settings.CheckpointLowerBound + 1
		settings.LiveBufferSize = settings.LiveBufferSize + 1
		settings.MaxRetryCount = settings.MaxRetryCount + 1
		settings.MessageTimeout = settings.MessageTimeout + 1
		settings.ExtraStatistics = !settings.ExtraStatistics
		settings.ResolveLinkTos = !settings.ResolveLinkTos

		err = clientInstance.UpdatePersistentSubscription(context.Background(), streamID, "Group 1", esdb.PersistentStreamSubscriptionOptions{
			Settings: &settings,
		})

		require.NoError(t, err)
	}
}

func updatePersistentStreamSubscription_ErrIfSubscriptionDoesNotExist(clientInstance *esdb.Client) TestCall {
	return func(t *testing.T) {
		streamID := NAME_GENERATOR.Generate()

		err := clientInstance.UpdatePersistentSubscription(context.Background(), streamID, "Group 1", esdb.PersistentStreamSubscriptionOptions{})

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

		esdbErr, ok := esdb.FromError(err)
		assert.False(t, ok)
		assert.Equal(t, esdbErr.Code(), esdb.ErrorResourceNotFound)
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
			StartFrom: esdb.Start{},
		})

		require.NoError(t, err)

		var receivedEvents sync.WaitGroup
		var droppedEvent sync.WaitGroup

		subscription, err := db.SubscribeToPersistentSubscription(
			context.Background(), streamID, groupName, esdb.SubscribeToPersistentSubscriptionOptions{
				BufferSize: 2,
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

					subscription.Ack(subEvent.EventAppeared.Event)

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

		err := client.CreatePersistentSubscriptionToAll(
			context.Background(),
			groupName,
			esdb.PersistentAllSubscriptionOptions{},
		)

		if err, ok := esdb.FromError(err); !ok {
			if err.Code() == esdb.ErrorUnsupportedFeature {
				t.Skip()
			}
		}

		require.NoError(t, err)
	}
}

func persistentAllUpdate(client *esdb.Client) TestCall {
	return func(t *testing.T) {
		groupName := NAME_GENERATOR.Generate()

		err := client.CreatePersistentSubscriptionToAll(
			context.Background(),
			groupName,
			esdb.PersistentAllSubscriptionOptions{},
		)

		if err, ok := esdb.FromError(err); !ok {
			if err.Code() == esdb.ErrorUnsupportedFeature {
				t.Skip()
			}
		}

		require.NoError(t, err)

		setts := esdb.SubscriptionSettingsDefault()
		setts.ResolveLinkTos = true

		err = client.UpdatePersistentSubscriptionToAll(context.Background(), groupName, esdb.PersistentAllSubscriptionOptions{
			Settings: &setts,
		})

		require.NoError(t, err)
	}
}

func persistentAllDelete(client *esdb.Client) TestCall {
	return func(t *testing.T) {
		groupName := NAME_GENERATOR.Generate()

		err := client.CreatePersistentSubscriptionToAll(
			context.Background(),
			groupName,
			esdb.PersistentAllSubscriptionOptions{},
		)

		if err, ok := esdb.FromError(err); !ok {
			if err.Code() == esdb.ErrorUnsupportedFeature {
				t.Skip()
			}
		}

		require.NoError(t, err)

		err = client.DeletePersistentSubscriptionToAll(context.Background(), groupName, esdb.DeletePersistentSubscriptionOptions{})

		require.NoError(t, err)
	}
}

func persistentReplayParkedMessages(client *esdb.Client) TestCall {
	return func(t *testing.T) {
		groupName := NAME_GENERATOR.Generate()
		streamName := NAME_GENERATOR.Generate()
		eventCount := 2

		err := client.CreatePersistentSubscription(context.Background(), streamName, groupName, esdb.PersistentStreamSubscriptionOptions{})

		require.NoError(t, err)

		sub, err := client.SubscribeToPersistentSubscription(context.Background(), streamName, groupName, esdb.SubscribeToPersistentSubscriptionOptions{})

		require.NoError(t, err)

		events := testCreateEvents(uint32(eventCount))

		_, err = client.AppendToStream(context.Background(), streamName, esdb.AppendToStreamOptions{}, events...)

		require.NoError(t, err)

		i := 0
		for i < eventCount {
			event := sub.Recv()

			if event.SubscriptionDropped != nil {
				t.FailNow()
			}

			if event.EventAppeared != nil {
				err = sub.Nack("because reasons", esdb.Nack_Park, event.EventAppeared.Event)
				require.NoError(t, err)
				i++
			}
		}

		// We let the server the time to park those events.
		time.Sleep(5 * time.Second)

		err = client.ReplayParkedMessages(context.Background(), streamName, groupName, esdb.ReplayParkedMessagesOptions{})

		i = 0
		for i < eventCount {
			event := sub.Recv()

			if event.SubscriptionDropped != nil {
				t.FailNow()
			}

			if event.EventAppeared != nil {
				err = sub.Ack(event.EventAppeared.Event)
				require.NoError(t, err)
				i++
			}
		}

		require.NoError(t, err)
	}
}

func persistentListAllSubs(client *esdb.Client) TestCall {
	return func(t *testing.T) {
		groupName := NAME_GENERATOR.Generate()
		streamName := NAME_GENERATOR.Generate()

		err := client.CreatePersistentSubscription(context.Background(), streamName, groupName, esdb.PersistentStreamSubscriptionOptions{})

		require.NoError(t, err)

		subs, err := client.ListAllPersistentSubscriptions(context.Background(), esdb.ListPersistentSubscriptionsOptions{})

		require.NoError(t, err)
		require.NotNil(t, subs)
		require.NotEmpty(t, subs)

		found := false
		for i := range subs {
			if subs[i].EventStreamId == streamName && subs[i].GroupName == groupName {
				found = true
				break
			}
		}

		require.True(t, found)
	}
}

func persistentListSubsForStream(client *esdb.Client) TestCall {
	return func(t *testing.T) {
		streamName := NAME_GENERATOR.Generate()
		groupNames := make(map[string]int)
		var err error
		count := 2

		for i := 0; i < count; i++ {
			name := NAME_GENERATOR.Generate()
			err = client.CreatePersistentSubscription(context.Background(), streamName, name, esdb.PersistentStreamSubscriptionOptions{})
			require.NoError(t, err)
			groupNames[name] = 0
		}

		require.NoError(t, err)

		subs, err := client.ListPersistentSubscriptionsForStream(context.Background(), streamName, esdb.ListPersistentSubscriptionsOptions{})

		require.NoError(t, err)
		require.NotNil(t, subs)
		require.NotEmpty(t, subs)

		found := 0
		for i := range subs {
			if subs[i].EventStreamId == streamName {
				if _, exists := groupNames[subs[i].GroupName]; exists {
					found++
				}
			}
		}

		require.Equal(t, 2, found)
	}
}

func persistentGetInfo(client *esdb.Client) TestCall {
	return func(t *testing.T) {
		streamName := NAME_GENERATOR.Generate()
		groupName := NAME_GENERATOR.Generate()

		err := client.CreatePersistentSubscription(context.Background(), streamName, groupName, esdb.PersistentStreamSubscriptionOptions{})

		require.NoError(t, err)

		sub, err := client.GetPersistentSubscriptionInfo(context.Background(), streamName, groupName, esdb.GetPersistentSubscriptionOptions{})

		require.NoError(t, err)

		require.Equal(t, streamName, sub.EventStreamId)
		require.Equal(t, groupName, sub.GroupName)
	}
}
