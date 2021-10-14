package esdb_test

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"sync"
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	uuid "github.com/gofrs/uuid"
	"github.com/stretchr/testify/require"
)

func TestStreamSubscriptionDeliversAllEventsInStreamAndListensForNewEvents(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()
	db := CreateTestClient(container, t)
	defer db.Close()

	streamID := "dataset20M-0"
	testEvent := createTestEvent()
	testEvent.EventID = uuid.FromStringOrNil("84c8e36c-4e64-11ea-8b59-b7f658acfc9f")

	var receivedEvents sync.WaitGroup
	var appendedEvents sync.WaitGroup

	subscription, err := db.SubscribeToStream(context.Background(), "dataset20M-0", esdb.SubscribeToStreamOptions{
		From: esdb.Start{},
	})

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
				appendedEvents.Done()
				break
			}
		}
	}()

	require.NoError(t, err)
	receivedEvents.Add(6_000)
	appendedEvents.Add(1)
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
	defer subscription.Close()
}

type Position struct {
	Prepare uint64 `json:"prepare"`
	Commit  uint64 `json:"commit"`
}

func TestAllSubscriptionWithFilterDeliversCorrectEvents(t *testing.T) {
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

	container := GetPrePopulatedDatabase()
	defer container.Close()
	db := CreateTestClient(container, t)
	defer db.Close()

	var receivedEvents sync.WaitGroup
	receivedEvents.Add(len(positions))

	subscription, err := db.SubscribeToAll(context.Background(), esdb.SubscribeToAllOptions{
		From: esdb.Start{},
		Filter: &esdb.SubscriptionFilter{
			Type:     esdb.EventFilterType,
			Prefixes: []string{"eventType-194"},
		},
	})

	go func() {
		current := 0
		for {
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
				receivedEvents.Done()
			}
		}
	}()

	require.NoError(t, err)
	timedOut := waitWithTimeout(&receivedEvents, time.Duration(5)*time.Second)
	require.False(t, timedOut, "Timed out while waiting for events via the subscription")
}

func TestConnectionClosing(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()
	db := CreateTestClient(container, t)
	defer db.Close()

	var receivedEvents sync.WaitGroup
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
					receivedEvents.Done()
					current++
				}

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
