package event_streams_with_prepopulated_database

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pivonroll/EventStore-Client-Go/errors"

	"github.com/stretchr/testify/require"

	"github.com/pivonroll/EventStore-Client-Go/event_streams"
)

func Test_StreamSubscription_DeliversAllEventsInStreamAndListensForNewEvents(t *testing.T) {
	client, closeFunc := initializeWithPrePopulatedDatabase(t)
	defer closeFunc()

	streamID := "dataset20M-0"
	testEvent := testCreateEvents(1)

	receivedEvents := sync.WaitGroup{}
	appendedEvents := sync.WaitGroup{}
	receivedEvents.Add(6_000)
	appendedEvents.Add(1)

	streamReader, err := client.SubscribeToStream(
		context.Background(),
		streamID,
		event_streams.ReadStreamRevisionStart{},
		false)
	defer streamReader.Close()
	require.NoError(t, err)

	go func() {
		current := 0
		for {
			subEvent, err := streamReader.ReadOne()
			require.NoError(t, err)

			if event, isEvent := subEvent.GetEvent(); isEvent {
				current++
				if current <= 6_000 {
					receivedEvents.Done()
					continue
				}

				require.Equal(t, testEvent[0].EventId, event.GetOriginalEvent().EventID)
				require.Equal(t, uint64(6_000), event.GetOriginalEvent().EventNumber)
				require.Equal(t, streamID, event.GetOriginalEvent().StreamId)
				require.Equal(t, testEvent[0].Data, event.GetOriginalEvent().Data)
				require.Equal(t, testEvent[0].UserMetadata, event.GetOriginalEvent().UserMetadata)
				appendedEvents.Done()
				break
			}
		}
	}()

	// Read events from stream
	timedOut := waitWithTimeout(&receivedEvents, time.Duration(5)*time.Second)
	require.False(t, timedOut, "Timed out waiting for initial set of events")

	// Write a new event
	writeResult, err := client.AppendToStream(context.Background(),
		streamID,
		event_streams.WriteStreamRevision{Revision: 5999},
		testEvent)
	require.NoError(t, err)
	require.False(t, writeResult.IsCurrentRevisionNoStream())
	require.Equal(t, uint64(6_000), writeResult.GetCurrentRevision())

	// Assert event was forwarded to the subscription
	timedOut = waitWithTimeout(&appendedEvents, time.Duration(5)*time.Second)
	require.False(t, timedOut, "Timed out waiting for the appended events")
}

func Test_SubscribeToStream_ConnectionClosing(t *testing.T) {
	client, closeFunc := initializeWithPrePopulatedDatabase(t)
	defer closeFunc()

	waitForClose := sync.WaitGroup{}
	waitForClose.Add(1)

	receivedEvents := sync.WaitGroup{}
	receivedEvents.Add(10)

	droppedEvent := sync.WaitGroup{}
	droppedEvent.Add(1)

	reader, err := client.SubscribeToStream(context.Background(),
		"dataset20M-0",
		event_streams.ReadStreamRevisionStart{},
		false)
	require.NoError(t, err)

	go func() {
		current := 1

		for {
			if current == 11 {
				waitForClose.Wait()
			}
			readEvent, err := reader.ReadOne()

			if err != nil && err.Code() == errors.CanceledErr {
				droppedEvent.Done()
				break
			}

			if _, isEvent := readEvent.GetEvent(); isEvent {
				if current <= 10 {
					receivedEvents.Done()
					current++
				}

				continue
			}
		}
	}()

	timedOut := waitWithTimeout(&receivedEvents, time.Duration(5)*time.Second)
	require.False(t, timedOut, "Timed out waiting for initial set of events")
	reader.Close()
	waitForClose.Done()
	timedOut = waitWithTimeout(&droppedEvent, time.Duration(5)*time.Second)
	require.False(t, timedOut, "Timed out waiting for dropped event")
}
