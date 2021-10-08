package projections_integration_test

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/pivonroll/EventStore-Client-Go/event_streams"
	"github.com/stretchr/testify/require"
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

func pushEventsToStream(t *testing.T,
	eventStreamsClient event_streams.Client,
	streamID string,
	events ...event_streams.ProposedEvent) {
	_, err := eventStreamsClient.AppendToStream(
		context.Background(),
		streamID,
		event_streams.WriteStreamRevisionNoStream{},
		events)

	require.NoError(t, err)
}
