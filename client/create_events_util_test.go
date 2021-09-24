package client_test

import (
	"context"
	"testing"

	"github.com/pivonroll/EventStore-Client-Go/client"
	"github.com/pivonroll/EventStore-Client-Go/event_streams"
	"github.com/stretchr/testify/require"
)

func testCreateEvent() event_streams.ProposedEvent {
	return createTestEventWithMetadataSize(4)
}

func testCreateEvents(count uint32) []event_streams.ProposedEvent {
	result := make([]event_streams.ProposedEvent, count)
	var i uint32 = 0
	for ; i < count; i++ {
		result[i] = testCreateEvent()
	}
	return result
}

func testCreateEventsWithMetadata(count uint32, metadataSize int) []event_streams.ProposedEvent {
	result := make([]event_streams.ProposedEvent, count)
	var i uint32 = 0
	for ; i < count; i++ {
		result[i] = createTestEventWithMetadataSize(metadataSize)
	}
	return result
}

func testCreateEventsWithBytesCap(bytesCap uint) []event_streams.ProposedEvent {
	byteCount := uint(0)
	result := make([]event_streams.ProposedEvent, 0)

	for {
		newEvent := testCreateEvent()
		byteCount += uint(len(newEvent.Data))

		if byteCount > bytesCap {
			break
		}
		result = append(result, newEvent)
	}

	return result
}

func pushEventToStream(t *testing.T, clientInstance *client.Client, streamID string) {
	testEvent := testCreateEvent()
	pushEventsToStream(t, clientInstance, streamID, testEvent)
}

func pushEventsToStream(t *testing.T,
	clientInstance *client.Client,
	streamID string,
	events ...event_streams.ProposedEvent) {
	_, err := clientInstance.EventStreams().AppendToStream(
		context.Background(),
		streamID,
		event_streams.AppendRequestExpectedStreamRevisionNoStream{},
		events)

	require.NoError(t, err)
}
