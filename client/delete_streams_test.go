package client_test

import (
	"context"
	"testing"

	"github.com/EventStore/EventStore-Client-Go/errors"

	"github.com/stretchr/testify/require"

	"github.com/EventStore/EventStore-Client-Go/event_streams"
)

func Test_DeleteStream_WithTimeout(t *testing.T) {
	container := GetEmptyDatabase()
	defer container.Close()
	client := CreateTestClient(container, t)
	defer func() {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}()

	t.Run("Any stream", func(t *testing.T) {
		streamName := "delete_any_stream"

		ctx := context.Background()
		timeoutCtx, cancelFunc := context.WithTimeout(ctx, 0)
		_, err := client.DeleteStream(timeoutCtx,
			streamName,
			event_streams.DeleteRequestExpectedStreamRevisionAny{})
		require.Equal(t, errors.DeadlineExceededErr, err.Code())

		defer cancelFunc()
	})

	t.Run("Any stream", func(t *testing.T) {
		streamName := "delete_stream_revision_0"

		ctx := context.Background()
		timeoutCtx, cancelFunc := context.WithTimeout(ctx, 0)
		_, err := client.DeleteStream(timeoutCtx,
			streamName,
			event_streams.DeleteRequestExpectedStreamRevision{
				Revision: 0,
			})
		require.Equal(t, errors.DeadlineExceededErr, err.Code())

		defer cancelFunc()
	})
}

func Test_DeleteStream(t *testing.T) {
	container := GetEmptyDatabase()
	defer container.Close()
	client := CreateTestClient(container, t)
	defer func() {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}()

	t.Run("Stream Does Not Exist, Revision NoStream", func(t *testing.T) {
		streamName := "stream_does_not_exist_no_stream"

		_, err := client.DeleteStream(context.Background(),
			streamName,
			event_streams.DeleteRequestExpectedStreamRevisionNoStream{})
		require.NoError(t, err)
	})

	t.Run("Stream Does Not Exist, Revision Any", func(t *testing.T) {
		streamName := "stream_does_not_exist_any"

		_, err := client.DeleteStream(context.Background(),
			streamName,
			event_streams.DeleteRequestExpectedStreamRevisionAny{})
		require.NoError(t, err)
	})

	t.Run("Stream Does Not Exist, Wrong Revision", func(t *testing.T) {
		streamName := "stream_does_not_exist_wrong_version"

		_, err := client.DeleteStream(context.Background(),
			streamName,
			event_streams.DeleteRequestExpectedStreamRevision{Revision: 0})
		require.Equal(t, errors.WrongExpectedStreamRevisionErr, err.Code())
	})

	t.Run("Stream with events returns position", func(t *testing.T) {
		streamName := "stream_with_events"

		event := createTestEvent()

		writeResult, err := client.AppendToStream(context.Background(),
			streamName,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			[]event_streams.ProposedEvent{event})
		require.NoError(t, err)

		currentRevision, _ := writeResult.GetCurrentRevision()
		deleteResult, err := client.DeleteStream(context.Background(),
			streamName,
			event_streams.DeleteRequestExpectedStreamRevision{Revision: currentRevision})
		require.NoError(t, err)

		tombstonePosition, isTombstonePosition := deleteResult.GetPosition()
		require.True(t, isTombstonePosition)
		writePosition, isWritePosition := writeResult.GetPosition()
		require.True(t, isWritePosition)

		require.True(t, tombstonePosition.GreaterThan(writePosition))
	})

	t.Run("Append After Deleted With Any", func(t *testing.T) {
		streamId := "append_after_deleted_with_any"

		writeResult, err := client.AppendToStream(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			testCreateEvents(1))
		require.NoError(t, err)

		currentRevision, _ := writeResult.GetCurrentRevision()

		_, err = client.DeleteStream(context.Background(),
			streamId,
			event_streams.DeleteRequestExpectedStreamRevision{Revision: currentRevision})
		require.NoError(t, err)

		events := testCreateEvents(3)

		writeResult, err = client.AppendToStream(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevisionAny{},
			events)
		require.NoError(t, err)

		currentRevision, _ = writeResult.GetCurrentRevision()
		require.EqualValues(t, 3, currentRevision)

		readEvents, err := client.ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadRequestDirectionForward,
			event_streams.ReadRequestOptionsStreamRevisionStart{},
			event_streams.ReadCountMax,
			false)
		require.NoError(t, err)

		require.Len(t, readEvents, 3)
		require.Equal(t, events, readEvents.ToProposedEvents())

		metaData, err := client.GetStreamMetadata(context.Background(), streamId)
		require.NoError(t, err)

		streamMetadata := metaData.GetStreamMetadata()
		require.EqualValues(t, 1, *streamMetadata.TruncateBefore)
		require.EqualValues(t, 1, metaData.GetMetaStreamRevision())
	})
}
