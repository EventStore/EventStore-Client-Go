package esdb_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/v4/esdb"
	"github.com/stretchr/testify/require"
)

func SubscriptionTests(t *testing.T, emptyDBClient *esdb.Client) {
	t.Run("SubscriptionTests", func(t *testing.T) {
		t.Run("streamSubscriptionDeliversAllEventsInStreamAndListensForNewEvents", streamSubscriptionDeliversAllEventsInStreamAndListensForNewEvents(emptyDBClient))
		t.Run("subscriptionAllFilter", subscriptionAllFilter(emptyDBClient))
		t.Run("connectionClosing", connectionClosing(emptyDBClient))
		t.Run("subscriptionToStreamCaughtUpMessage", subscriptionToStreamCaughtUpMessage(emptyDBClient))
	})
}

func subscriptionToStreamCaughtUpMessage(db *esdb.Client) TestCall {
	const minSupportedVersion = 23
	const expectedEventCount = 16
	const testTimeout = 1 * time.Minute

	return func(t *testing.T) {
		if db == nil {
			t.Skip("Database client is nil")
		}

		esdbVersion, err := db.GetServerVersion()
		require.NoError(t, err, "Error getting server version")

		if esdbVersion.Major < minSupportedVersion {
			t.Skip("CaughtUp message is not supported in this version of EventStoreDB")
		}

		ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
		defer cancel()

		streamID := NAME_GENERATOR.Generate()
		testEvents := createTestEvents(expectedEventCount)

		_, err = db.AppendToStream(context.Background(), streamID, esdb.AppendToStreamOptions{}, testEvents...)
		require.NoError(t, err)

		subscription, err := db.SubscribeToStream(ctx, streamID, esdb.SubscribeToStreamOptions{From: esdb.Start{}})
		require.NoError(t, err)
		defer subscription.Close()

		var caughtUpReceived sync.WaitGroup
		caughtUpReceived.Add(1)

		go func() {
			var count uint64 = 0
			defer caughtUpReceived.Done()
			allEventsAcknowledged := false

			for {
				select {
				case <-ctx.Done():
					t.Error("Context timed out before receiving CaughtUp message")
					return
				default:
					event := subscription.Recv()

					if event.EventAppeared != nil {
						count++

						if count == expectedEventCount {
							allEventsAcknowledged = true
						}

						continue
					}

					if allEventsAcknowledged && event.CaughtUp != nil {
						require.True(t, count >= expectedEventCount, "Did not receive the exact number of expected events before CaughtUp")
						return
					}
				}
			}
		}()

		caughtUpTimedOut := waitWithTimeout(&caughtUpReceived, testTimeout)
		require.False(t, caughtUpTimedOut, "Timed out waiting for CaughtUp message")
	}
}

func streamSubscriptionDeliversAllEventsInStreamAndListensForNewEvents(db *esdb.Client) TestCall {
	return func(t *testing.T) {
		streamID := NAME_GENERATOR.Generate()
		eventCount := 64
		testEvents := createTestEvents(eventCount)

		writeResult, err := db.AppendToStream(context.Background(), streamID, esdb.AppendToStreamOptions{}, testEvents...)
		require.NoError(t, err)

		var receivedEvents sync.WaitGroup
		var appendedEvents sync.WaitGroup

		subscription, err := db.SubscribeToStream(context.Background(), streamID, esdb.SubscribeToStreamOptions{
			From: esdb.Start{},
		})

		require.NoError(t, err)
		defer subscription.Close()
		receivedEvents.Add(eventCount)
		appendedEvents.Add(1)

		go func() {
			for i := 0; i < eventCount; {
				subEvent := subscription.Recv()

				if subEvent.SubscriptionDropped != nil {
					break
				}

				if subEvent.EventAppeared != nil {
					i++
					receivedEvents.Done()
				}
			}

			appendedEvents.Done()
		}()

		timedOut := waitWithTimeout(&receivedEvents, time.Duration(5)*time.Second)
		require.False(t, timedOut, "Timed out waiting for initial set of events")

		// Write a new event
		opts2 := esdb.AppendToStreamOptions{
			ExpectedRevision: esdb.Revision(writeResult.NextExpectedVersion),
		}
		_, err = db.AppendToStream(context.Background(), streamID, opts2, createTestEvent())
		require.NoError(t, err)

		// Assert event was forwarded to the subscription
		timedOut = waitWithTimeout(&appendedEvents, time.Duration(5)*time.Second)
		require.False(t, timedOut, "Timed out waiting for the appended events")
	}
}

type Position struct {
	Prepare uint64 `json:"prepare"`
	Commit  uint64 `json:"commit"`
}

func subscriptionAllFilter(db *esdb.Client) TestCall {
	return func(t *testing.T) {
		sub, err := db.SubscribeToAll(context.Background(), esdb.SubscribeToAllOptions{From: esdb.Start{}, Filter: esdb.ExcludeSystemEventsFilter()})

		if err != nil {
			t.Error(err)
		}

		var completed sync.WaitGroup
		completed.Add(1)

		go func() {
			for {
				event := sub.Recv()

				if event.EventAppeared != nil {
					if strings.HasPrefix(event.EventAppeared.OriginalEvent().EventType, "$") {
						t.Fatalf("We should not have system events!")
					}

					completed.Done()
					break
				}

				if event.SubscriptionDropped != nil {
					break
				}
			}
		}()

		timedOut := waitWithTimeout(&completed, time.Duration(30)*time.Second)
		require.False(t, timedOut, "Timed out waiting for filtered subscription completion")
	}
}

func connectionClosing(db *esdb.Client) TestCall {
	return func(t *testing.T) {
		streamID := NAME_GENERATOR.Generate()
		eventCount := 128
		testEvents := createTestEvents(eventCount)

		_, err := db.AppendToStream(context.Background(), streamID, esdb.AppendToStreamOptions{}, testEvents...)
		require.NoError(t, err)

		var droppedEvent sync.WaitGroup

		subscription, err := db.SubscribeToStream(context.Background(), streamID, esdb.SubscribeToStreamOptions{
			From: esdb.Start{},
		})

		go func() {
			current := 0

			for {
				subEvent := subscription.Recv()

				if subEvent.EventAppeared != nil {
					if current <= 10 {
						current++
					} else {
						subscription.Close()
					}

					continue
				}

				if subEvent.SubscriptionDropped != nil {
					break
				}
			}

			droppedEvent.Done()
		}()

		require.NoError(t, err)
		droppedEvent.Add(1)
		timedOut := waitWithTimeout(&droppedEvent, time.Duration(5)*time.Second)
		require.False(t, timedOut, "Timed out waiting for dropped event")
	}
}

func waitWithTimeout(wg *sync.WaitGroup, duration time.Duration) bool {
	channel := make(chan struct{})
	go func() {
		defer close(channel)
		wg.Wait()
	}()
	select {
	case <-channel:
		return false
	case <-time.After(duration):
		return true
	}
}
