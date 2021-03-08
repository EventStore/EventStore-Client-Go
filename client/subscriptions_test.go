package client_test

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"sync"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/require"

	"github.com/EventStore/EventStore-Client-Go/client/filtering"
	"github.com/EventStore/EventStore-Client-Go/messages"
	"github.com/EventStore/EventStore-Client-Go/position"
	"github.com/EventStore/EventStore-Client-Go/streamrevision"
	stream_revision "github.com/EventStore/EventStore-Client-Go/streamrevision"
)

func TestStreamSubscriptionDeliversAllowsCancellationDuringStream(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()
	client := CreateTestClient(container, t)
	defer client.Close()

	var cancellation sync.WaitGroup
	cancellation.Add(1)
	subscription, err := client.SubscribeToStream(context.Background(), "dataset20M-0", streamrevision.StreamRevisionStart, false,
		nil,
		nil,
		func(reason string) {
			cancellation.Done()
		})

	require.NoError(t, err)
	subscription.Start()
	subscription.Stop()
	timedOut := waitWithTimeout(&cancellation, time.Duration(5)*time.Second)
	require.False(t, timedOut, "Timed out waiting for subscription cancellation")
}

func TestStreamSubscriptionDeliversAllEventsInStreamAndListensForNewEvents(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()
	client := CreateTestClient(container, t)
	defer client.Close()

	streamID := "dataset20M-0"
	testEvent := messages.ProposedEvent{
		EventID:      uuid.FromStringOrNil("84c8e36c-4e64-11ea-8b59-b7f658acfc9f"),
		EventType:    "TestEvent",
		ContentType:  "application/octet-stream",
		UserMetadata: []byte{0xd, 0xe, 0xa, 0xd},
		Data:         []byte{0xb, 0xe, 0xe, 0xf},
	}

	current := 0
	var receivedEvents sync.WaitGroup
	var appendedEvents sync.WaitGroup
	subscription, err := client.SubscribeToStream(context.Background(), "dataset20M-0", streamrevision.StreamRevisionStart, false,
		func(event messages.RecordedEvent) {
			current++
			if current <= 5999 {
				receivedEvents.Done()
			} else {
				require.Equal(t, testEvent.EventID, event.EventID)
				require.Equal(t, uint64(6000), event.EventNumber)
				require.Equal(t, streamID, event.StreamID)
				require.Equal(t, testEvent.Data, event.Data)
				require.Equal(t, testEvent.UserMetadata, event.UserMetadata)
				appendedEvents.Done()
			}
		}, nil, nil)

	require.NoError(t, err)
	receivedEvents.Add(5999)
	appendedEvents.Add(1)
	subscription.Start()
	timedOut := waitWithTimeout(&receivedEvents, time.Duration(5)*time.Second)
	require.False(t, timedOut, "Timed out waiting for initial set of events")

	// Write a new event
	writeResult, err := client.AppendToStream(context.Background(), streamID, stream_revision.NewStreamRevision(5999), []messages.ProposedEvent{testEvent})
	require.NoError(t, err)
	require.Equal(t, uint64(6000), writeResult.NextExpectedVersion)

	// Assert event was forwarded to the subscription
	timedOut = waitWithTimeout(&appendedEvents, time.Duration(5)*time.Second)
	require.False(t, timedOut, "Timed out waiting for the appended events")
	defer subscription.Stop()
}

func TestAllSubscriptionDeliversAllowsCancellationDuringStream(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()
	client := CreateTestClient(container, t)
	defer client.Close()

	var cancellation sync.WaitGroup
	cancellation.Add(1)
	subscription, err := client.SubscribeToAll(context.Background(), position.StartPosition, false,
		nil,
		nil,
		func(reason string) {
			cancellation.Done()
		})

	require.NoError(t, err)
	subscription.Start()
	subscription.Stop()
	timedOut := waitWithTimeout(&cancellation, time.Duration(5)*time.Second)
	require.False(t, timedOut, "Timed out waiting for subscription cancellation")
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
	client := CreateTestClient(container, t)
	client.Connect()
	defer client.Close()

	var receivedEvents sync.WaitGroup
	var cancellation sync.WaitGroup
	receivedEvents.Add(len(positions))
	cancellation.Add(1)

	filter := filtering.NewEventPrefixFilter([]string{"eventType-194"})
	filterOptions := filtering.NewDefaultSubscriptionFilterOptions(filter)

	current := 0

	subscription, err := client.SubscribeToAllFiltered(context.Background(), position.StartPosition, false, filterOptions,
		func(event messages.RecordedEvent) {
			require.Equal(t, versions[current], event.EventNumber)
			require.Equal(t, positions[current].Commit, event.Position.Commit)
			require.Equal(t, positions[current].Prepare, event.Position.Prepare)
			current++
			receivedEvents.Done()
		}, nil, func(reason string) {
			cancellation.Done()
		})

	require.NoError(t, err)
	subscription.Start()
	timedOut := waitWithTimeout(&receivedEvents, time.Duration(5)*time.Second)
	require.False(t, timedOut, "Timed out while waiting for events via the subscription")
	subscription.Stop()
	timedOut = waitWithTimeout(&cancellation, time.Duration(5)*time.Second)
	require.False(t, timedOut, "Timed out waiting for subscription cancellation")
}

func TestStreamSubscriptionAndAppendToSameStream(t *testing.T) {
	container := GetEmptyDatabase()
	defer container.Close()
	client := CreateTestClient(container, t)
	defer client.Close()

	streamID := "stream-with-multiple-subscriptions"
	birthEvent := createTypedTestEvent("birthEvent")
	firstEvent := createTypedTestEvent("firstEvent")
	secondEvent := createTypedTestEvent("secondEvent")

	_, err := client.AppendToStream(context.Background(), streamID, stream_revision.StreamRevisionNoStream, []messages.ProposedEvent{
		birthEvent,
		firstEvent,
	})
	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}

	var firstEventReceived, secondEventReceived sync.WaitGroup
	firstEventReceived.Add(1)
	secondEventReceived.Add(1)

	subscriptionOne, err := client.SubscribeToStream(context.Background(), streamID, streamrevision.StreamRevisionStart, false,
		func(event messages.RecordedEvent) {
			if event.EventType == firstEvent.EventType {
				firstEventReceived.Done()
			}
			if event.EventType == secondEvent.EventType {
				secondEventReceived.Done()
			}
		}, nil,
		nil)
	require.NoError(t, err)

	err = subscriptionOne.Start()
	require.NoError(t, err, "failed to start subscription one")

	timedOut := waitWithTimeout(&firstEventReceived, 5*time.Second)
	require.False(t, timedOut, "Timed out waiting for first event")

	_, err = client.AppendToStream(context.Background(), streamID, 1, []messages.ProposedEvent{
		secondEvent,
	})

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}

	timedOut = waitWithTimeout(&secondEventReceived, 5*time.Second)
	require.False(t, timedOut, "Timed out waiting for second event")
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
