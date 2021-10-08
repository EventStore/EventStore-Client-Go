package append_test

import (
	"strings"

	"github.com/google/uuid"
	"github.com/pivonroll/EventStore-Client-Go/event_streams"
)

func createTestEventWithMetadataSize(metadataSize int) event_streams.ProposedEvent {
	return event_streams.ProposedEvent{
		EventId:      uuid.Must(uuid.NewRandom()),
		EventType:    "TestEvent",
		ContentType:  "application/octet-stream",
		UserMetadata: []byte(strings.Repeat("$", metadataSize)),
		Data:         []byte{0xb, 0xe, 0xe, 0xf},
	}
}

func testCreateEvent() event_streams.ProposedEvent {
	return createTestEventWithMetadataSize(4)
}

func testCreateEvents(count uint32) event_streams.ProposedEventList {
	result := make(event_streams.ProposedEventList, count)
	var i uint32 = 0
	for ; i < count; i++ {
		result[i] = testCreateEvent()
	}
	return result
}

func testCreateEventsWithBytesCap(bytesCap uint) event_streams.ProposedEventList {
	byteCount := uint(0)
	result := make(event_streams.ProposedEventList, 0)

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
