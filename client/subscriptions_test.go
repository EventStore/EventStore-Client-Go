package client_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/client/filtering"
	"github.com/EventStore/EventStore-Client-Go/messages"
	"github.com/EventStore/EventStore-Client-Go/position"
	"github.com/EventStore/EventStore-Client-Go/streamrevision"
	stream_revision "github.com/EventStore/EventStore-Client-Go/streamrevision"
	uuid "github.com/gofrs/uuid"
	"github.com/stretchr/testify/require"
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
	container := GetPrePopulatedDatabase()
	defer container.Close()
	client := CreateTestClient(container, t)
	client.Connect()
	defer client.Close()

	var receivedEvents sync.WaitGroup
	var cancellation sync.WaitGroup
	receivedEvents.Add(130)
	cancellation.Add(1)

	filter := filtering.NewEventPrefixFilter([]string{"eventType-194"})
	filterOptions := filtering.NewDefaultSubscriptionFilterOptions(filter)

	subscription, err := client.SubscribeFilteredToAll(context.Background(), position.StartPosition, false, filterOptions,
		func(event messages.RecordedEvent) {
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
