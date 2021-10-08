package batch_append_to_stream

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
				streamName,
				event_streams.WriteStreamRevisionNoStream{},
				[]event_streams.ProposedEvent{},
				5,
				time.Now().Add(time.Second*10),
			)
			require.NoError(t, err)
		}
		_, err := client.ReadStreamEvents(context.Background(),
			streamName,
			event_streams.ReadDirectionForward,
			event_streams.ReadStreamRevisionStart{},
			2,
			false)
		require.Equal(t, err.Code(), errors.StreamNotFoundErr)
	})

	t.Run("With Expected Revision Any", func(t *testing.T) {
		streamName := "batch_append_revision_any"

		iterations := 2
		for ; iterations > 0; iterations-- {
			writeResult, err := client.BatchAppendToStream(context.Background(),
				streamName,
				event_streams.WriteStreamRevisionAny{},
				[]event_streams.ProposedEvent{},
				1,
				time.Now().Add(time.Second*10),
			)
			require.NoError(t, err)
			require.True(t, writeResult.IsCurrentRevisionNoStream())
		}

		_, err := client.ReadStreamEvents(context.Background(),
			streamName,
			event_streams.ReadDirectionForward,
			event_streams.ReadStreamRevisionStart{},
			2,
			false)
		require.Equal(t, err.Code(), errors.StreamNotFoundErr)
	})
}

func Test_BatchAppendToNonExistingStream_WithExpectedRevision(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	t.Run("Append To Non-Existing Stream With Any", func(t *testing.T) {
		streamName := "batch_append_to_non_existing_stream_any"

		testEvent := testCreateEvent()

		writeResult, err := client.BatchAppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionAny{},
			[]event_streams.ProposedEvent{testEvent},
			1,
			time.Now().Add(time.Second*10))
		require.NoError(t, err)
		require.False(t, writeResult.IsCurrentRevisionNoStream())
		require.EqualValues(t, 0, writeResult.GetCurrentRevision())

		events, err := client.ReadStreamEvents(context.Background(),
			streamName,
			event_streams.ReadDirectionForward,
			event_streams.ReadStreamRevisionStart{},
			2,
			false)
		require.NoError(t, err)
		require.Len(t, events, 1)
	})

	t.Run("Append To Non-Existing Stream With NoStream", func(t *testing.T) {
		streamName := "batch_append_to_non_existing_stream_no_stream_append_multiple_at_once"

		testEvents := testCreateEvents(100)

		writeResult, err := client.BatchAppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionAny{},
			testEvents,
			50,
			time.Now().Add(time.Second*10),
		)
		require.NoError(t, err)
		require.False(t, writeResult.IsCurrentRevisionNoStream())
		require.EqualValues(t, 99, writeResult.GetCurrentRevision())
	})
}

func Test_BatchAppendToExistingStream_WithExpectedRevision(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	t.Run("Append To Existing Stream With Correct Revision", func(t *testing.T) {
		streamName := "batch_append_to_existing_stream_with_correct_revision"

		testEvents := testCreateEvents(2)

		writeResult, err := client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionNoStream{},
			testEvents)
		require.NoError(t, err)
		require.EqualValues(t, 1, writeResult.GetCurrentRevision())

		batchTestEvents := testCreateEvents(100)

		batchWriteResult, err := client.BatchAppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevision{Revision: 1},
			batchTestEvents,
			50,
			time.Now().Add(time.Second*10),
		)

		require.NoError(t, err)
		require.False(t, batchWriteResult.IsCurrentRevisionNoStream())
		require.EqualValues(t, 101, batchWriteResult.GetCurrentRevision())
	})

	t.Run("Append To Existing Stream With Incorrect Revision", func(t *testing.T) {
		streamName := "batch_append_to_existing_stream_with_incorrect_revision"

		testEvents := testCreateEvents(2)

		writeResult, err := client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionNoStream{},
			testEvents)
		require.NoError(t, err)
		require.EqualValues(t, 1, writeResult.GetCurrentRevision())

		batchTestEvents := testCreateEvents(100)

		_, err = client.BatchAppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevision{Revision: 2},
			batchTestEvents,
			50,
			time.Now().Add(time.Second*10))

		require.Equal(t, errors.WrongExpectedStreamRevisionErr, err.Code())
	})
}

func Test_BatchAppend_WithIncorrectCredentials(t *testing.T) {
	client, closeFunc := initializeContainerAndClientWithCredentials(t,
		"wrong_user_name", "wrong_password", nil)
	defer closeFunc()

	streamName := "batch_append_to_non_existing_stream_no_stream_append_multiple_at_once"

	testEvents := testCreateEvents(100)

	_, err := client.BatchAppendToStream(context.Background(),
		streamName,
		event_streams.WriteStreamRevisionAny{},
		testEvents,
		50,
		time.Now().Add(time.Second*10))
	require.Equal(t, errors.UnauthenticatedErr, err.Code())
}
