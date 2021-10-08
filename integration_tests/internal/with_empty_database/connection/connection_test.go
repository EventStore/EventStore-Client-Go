package connection_integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/google/uuid"
	"github.com/pivonroll/EventStore-Client-Go/connection"
	"github.com/pivonroll/EventStore-Client-Go/event_streams"
	"github.com/stretchr/testify/assert"
)

func Test_CloseConnection(t *testing.T) {
	container, grpcClient, eventStreamClient := initializeContainerAndClient(t, nil)
	defer container.Close()

	testEvent := event_streams.ProposedEvent{
		EventId:      uuid.MustParse("38fffbc2-339e-11ea-8c7b-784f43837872"),
		EventType:    "TestEvent",
		ContentType:  "application/octet-stream",
		UserMetadata: []byte{0xd, 0xe, 0xa, 0xd},
		Data:         []byte{0xb, 0xe, 0xe, 0xf},
	}
	proposedEvents := []event_streams.ProposedEvent{
		testEvent,
	}

	streamID, _ := uuid.NewRandom()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
	defer cancel()
	_, err := eventStreamClient.AppendToStream(ctx,
		streamID.String(),
		event_streams.WriteStreamRevisionNoStream{},
		proposedEvents)
	require.NoError(t, err)

	grpcClient.Close()
	_, err = eventStreamClient.AppendToStream(ctx,
		streamID.String(),
		event_streams.WriteStreamRevisionAny{},
		proposedEvents)

	assert.Equal(t, connection.EsdbConnectionIsClosed, err.Code())
}
