package esdb_test

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"io/ioutil"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/v3/esdb"
	"github.com/stretchr/testify/require"
)

func SubscriptionTests(t *testing.T, emptyDBClient *esdb.Client, populatedDBClient *esdb.Client) {
	t.Run("SubscriptionTests", func(t *testing.T) {
		t.Run("streamSubscriptionDeliversAllEventsInStreamAndListensForNewEvents", streamSubscriptionDeliversAllEventsInStreamAndListensForNewEvents(populatedDBClient))
		t.Run("allSubscriptionWithFilterDeliversCorrectEvents", allSubscriptionWithFilterDeliversCorrectEvents(populatedDBClient))
		t.Run("subscriptionAllFilter", subscriptionAllFilter(emptyDBClient))
		t.Run("connectionClosing", connectionClosing(populatedDBClient))
		t.Run("subscriptionAllWithCredentialsOverride", subscriptionAllWithCredentialsOverride(populatedDBClient))
		t.Run("subscriptionToStreamCaughtUpMessage", subscriptionToStreamCaughtUpMessage(populatedDBClient))
	})
}

func subscriptionToStreamCaughtUpMessage(db *esdb.Client) TestCall {
	const minSupportedVersion = 23
	const expectedEventCount = 6_000
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

		streamID := "dataset20M-0"
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
		if db == nil {
			t.Skip()
		}

		streamID := "dataset20M-0"
		testEvent := createTestEvent()
		testEvent.EventID = uuid.MustParse("84c8e36c-4e64-11ea-8b59-b7f658acfc9f")

		var receivedEvents sync.WaitGroup
		var appendedEvents sync.WaitGroup

		subscription, err := db.SubscribeToStream(context.Background(), "dataset20M-0", esdb.SubscribeToStreamOptions{
			From: esdb.Start{},
		})

		require.NoError(t, err)
		defer subscription.Close()
		receivedEvents.Add(6_000)
		appendedEvents.Add(1)

		go func() {
			current := 0
			for {
				subEvent := subscription.Recv()

				if subEvent.EventAppeared != nil {
					current++
					if current <= 6_000 {
						receivedEvents.Done()
						continue
					}

					event := subEvent.EventAppeared
					require.Equal(t, testEvent.EventID, event.OriginalEvent().EventID)
					require.Equal(t, uint64(6_000), event.OriginalEvent().EventNumber)
					require.Equal(t, streamID, event.OriginalEvent().StreamID)
					require.Equal(t, testEvent.Data, event.OriginalEvent().Data)
					require.Equal(t, testEvent.Metadata, event.OriginalEvent().UserMetadata)
					break
				}
			}
			appendedEvents.Done()
		}()

		timedOut := waitWithTimeout(&receivedEvents, time.Duration(5)*time.Second)
		require.False(t, timedOut, "Timed out waiting for initial set of events")

		// Write a new event
		opts2 := esdb.AppendToStreamOptions{
			ExpectedRevision: esdb.Revision(5_999),
		}
		writeResult, err := db.AppendToStream(context.Background(), streamID, opts2, testEvent)
		require.NoError(t, err)
		require.Equal(t, uint64(6_000), writeResult.NextExpectedVersion)

		// Assert event was forwarded to the subscription
		timedOut = waitWithTimeout(&appendedEvents, time.Duration(5)*time.Second)
		require.False(t, timedOut, "Timed out waiting for the appended events")
	}
}

type Position struct {
	Prepare uint64 `json:"prepare"`
	Commit  uint64 `json:"commit"`
}

func allSubscriptionWithFilterDeliversCorrectEvents(db *esdb.Client) TestCall {
	return func(t *testing.T) {
		if db == nil {
			t.Skip()
		}

		positionsContent, err := ioutil.ReadFile("../resources/test/all-positions-filtered-stream-194-e0-e30.json")
		require.NoError(t, err)
		versionsContent, err := ioutil.ReadFile("../resources/test/all-versions-filtered-stream-194-e0-e30.json")
		require.NoError(t, err)
		var positions []Position
		var versions []uint64
		err = json.Unmarshal(positionsContent, &positions)
		require.NoError(t, err)
		err = json.Unmarshal(versionsContent, &versions)
		require.NoError(t, err)

		var receivedEvents sync.WaitGroup
		receivedEvents.Add(1)

		subscription, err := db.SubscribeToAll(context.Background(), esdb.SubscribeToAllOptions{
			From: esdb.Start{},
			Filter: &esdb.SubscriptionFilter{
				Type:     esdb.EventFilterType,
				Prefixes: []string{"eventType-194"},
			},
		})

		defer subscription.Close()

		go func() {
			current := 0
			for current <= len(versions)-1 {
				subEvent := subscription.Recv()

				if subEvent.SubscriptionDropped != nil {
					break
				}

				if subEvent.EventAppeared != nil {
					event := subEvent.EventAppeared

					require.Equal(t, versions[current], event.OriginalEvent().EventNumber)
					require.Equal(t, positions[current].Commit, event.OriginalEvent().Position.Commit)
					require.Equal(t, positions[current].Prepare, event.OriginalEvent().Position.Prepare)
					current++
				}
			}

			if current > len(versions)-1 {
				receivedEvents.Done()
			}
		}()

		require.NoError(t, err)
		timedOut := waitWithTimeout(&receivedEvents, time.Duration(5)*time.Second)
		require.False(t, timedOut, "Timed out while waiting for events via the subscription")
	}
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
			}
		}()

		timedOut := waitWithTimeout(&completed, time.Duration(30)*time.Second)
		require.False(t, timedOut, "Timed out waiting for filtered subscription completion")
	}
}

func connectionClosing(db *esdb.Client) TestCall {
	return func(t *testing.T) {
		if db == nil {
			t.Skip()
		}

		var droppedEvent sync.WaitGroup

		subscription, err := db.SubscribeToStream(context.Background(), "dataset20M-0", esdb.SubscribeToStreamOptions{
			From: esdb.Start{},
		})

		go func() {
			current := 1

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

func subscriptionAllWithCredentialsOverride(db *esdb.Client) TestCall {
	return func(t *testing.T) {
		if db == nil {
			t.Skip()
		}

		opts := esdb.SubscribeToAllOptions{
			Authenticated: &esdb.Credentials{
				Login:    "admin",
				Password: "changeit",
			},
			From:   esdb.Start{},
			Filter: esdb.ExcludeSystemEventsFilter(),
		}
		_, err := db.SubscribeToAll(context.Background(), opts)

		if err != nil {
			t.Error(err)
		}
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
