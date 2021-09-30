package event_streams_integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/pivonroll/EventStore-Client-Go/errors"
	"github.com/pivonroll/EventStore-Client-Go/event_streams"
	"github.com/stretchr/testify/require"
)

func Test_BatchAppendZeroEvents_ToNonExistingStream(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	t.Run("With Expected Revision NoStream", func(t *testing.T) {
		streamName := "batch_append_revision_no_stream"

		iterations := 2
		for ; iterations > 0; iterations-- {
			_, err := client.BatchAppendToStream(context.Background(),
				event_streams.BatchAppendRequestOptions{
					StreamIdentifier:       streamName,
					ExpectedStreamPosition: event_streams.BatchAppendExpectedStreamPositionNoStream{},
					Deadline:               time.Now().Add(time.Second * 10),
				},
				[]event_streams.ProposedEvent{},
				5)
			require.NoError(t, err)
		}
		_, err := client.ReadStreamEvents(context.Background(),
			streamName,
			event_streams.ReadRequestDirectionForward,
			event_streams.ReadRequestOptionsStreamRevisionStart{},
			2,
			false)
		require.Equal(t, err.Code(), errors.StreamNotFoundErr)
	})

	t.Run("With Expected Revision Any", func(t *testing.T) {
		streamName := "batch_append_revision_any"

		iterations := 2
		for ; iterations > 0; iterations-- {
			writeResult, err := client.BatchAppendToStream(context.Background(),
				event_streams.BatchAppendRequestOptions{
					StreamIdentifier:       streamName,
					ExpectedStreamPosition: event_streams.BatchAppendExpectedStreamPositionAny{},
					Deadline:               time.Now().Add(time.Second * 10),
				},
				[]event_streams.ProposedEvent{},
				1)
			require.NoError(t, err)
			require.True(t, writeResult.GetRevisionNoStream())
		}

		_, err := client.ReadStreamEvents(context.Background(),
			streamName,
			event_streams.ReadRequestDirectionForward,
			event_streams.ReadRequestOptionsStreamRevisionStart{},
			2,
			false)
		require.Equal(t, err.Code(), errors.StreamNotFoundErr)
	})
}

func Test_BatchAppendToNonExistingStream_WithExpectedRevision(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	t.Run("With Expected Revision Any", func(t *testing.T) {
		streamName := "batch_append_to_non_existing_stream_any"

		testEvent := testCreateEvent()

		writeResult, err := client.BatchAppendToStream(context.Background(),
			event_streams.BatchAppendRequestOptions{
				StreamIdentifier:       streamName,
				ExpectedStreamPosition: event_streams.BatchAppendExpectedStreamPositionAny{},
				Deadline:               time.Now().Add(time.Second * 10),
			},
			[]event_streams.ProposedEvent{testEvent},
			1)
		require.NoError(t, err)
		require.False(t, writeResult.GetRevisionNoStream())
		require.EqualValues(t, 0, writeResult.GetRevision())

		events, err := client.ReadStreamEvents(context.Background(),
			streamName,
			event_streams.ReadRequestDirectionForward,
			event_streams.ReadRequestOptionsStreamRevisionStart{},
			2,
			false)
		require.NoError(t, err)
		require.Len(t, events, 1)
	})

	t.Run("With Expected Revision NoStream, Append Multiple At Once", func(t *testing.T) {
		streamName := "batch_append_to_non_existing_stream_no_stream_append_multiple_at_once"

		testEvents := testCreateEvents(100)

		writeResult, err := client.BatchAppendToStream(context.Background(),
			event_streams.BatchAppendRequestOptions{
				StreamIdentifier:       streamName,
				ExpectedStreamPosition: event_streams.BatchAppendExpectedStreamPositionAny{},
				Deadline:               time.Now().Add(time.Second * 10),
			},
			testEvents,
			50)
		require.NoError(t, err)
		require.False(t, writeResult.GetRevisionNoStream())
		require.EqualValues(t, 99, writeResult.GetRevision())
	})
}
