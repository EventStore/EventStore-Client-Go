package client_test

import (
	"context"
	"testing"

	"github.com/pivonroll/EventStore-Client-Go/errors"
	"github.com/pivonroll/EventStore-Client-Go/event_streams"
	"github.com/stretchr/testify/require"
)

func Test_TombstoneStream_WithTimeout(t *testing.T) {
	container := getEmptyDatabase()
	defer container.Close()
	client := createClientConnectedToContainer(container, t)
	defer func() {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}()

	t.Run("Any stream", func(t *testing.T) {
		streamName := "tombstone_any_stream"

		ctx := context.Background()
		timeoutCtx, cancelFunc := context.WithTimeout(ctx, 0)
		_, err := client.EventStreams().TombstoneStream(timeoutCtx,
			streamName,
			event_streams.TombstoneRequestExpectedStreamRevisionAny{})
		require.Equal(t, errors.DeadlineExceededErr, err.Code())

		defer cancelFunc()
	})

	t.Run("Any stream", func(t *testing.T) {
		streamName := "tombstone_stream_revision_0"

		ctx := context.Background()
		timeoutCtx, cancelFunc := context.WithTimeout(ctx, 0)
		_, err := client.EventStreams().TombstoneStream(timeoutCtx,
			streamName,
			event_streams.TombstoneRequestExpectedStreamRevision{
				Revision: 0,
			})
		require.Equal(t, errors.DeadlineExceededErr, err.Code())

		defer cancelFunc()
	})
}

func Test_TombstoneStream(t *testing.T) {
	container := getEmptyDatabase()
	defer container.Close()
	client := createClientConnectedToContainer(container, t)
	defer func() {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}()

	t.Run("Stream Does Not Exist, Revision NoStream", func(t *testing.T) {
		streamName := "stream_does_not_exist_no_stream"

		_, err := client.EventStreams().TombstoneStream(context.Background(),
			streamName,
			event_streams.TombstoneRequestExpectedStreamRevisionNoStream{})
		require.NoError(t, err)
	})

	t.Run("Stream Does Not Exist, Revision Any", func(t *testing.T) {
		streamName := "stream_does_not_exist_any"

		_, err := client.EventStreams().TombstoneStream(context.Background(),
			streamName,
			event_streams.TombstoneRequestExpectedStreamRevisionAny{})
		require.NoError(t, err)
	})

	t.Run("Stream Does Not Exist, Wrong Revision", func(t *testing.T) {
		streamName := "stream_does_not_exist_wrong_version"

		_, err := client.EventStreams().TombstoneStream(context.Background(),
			streamName,
			event_streams.TombstoneRequestExpectedStreamRevision{Revision: 0})
		require.Equal(t, errors.WrongExpectedStreamRevisionErr, err.Code())
	})

	t.Run("If Stream Is Already Tombstoned", func(t *testing.T) {
		streamName := "already_tombstoned_stream"

		_, err := client.EventStreams().TombstoneStream(context.Background(),
			streamName,
			event_streams.TombstoneRequestExpectedStreamRevisionNoStream{})
		require.NoError(t, err)

		_, err = client.EventStreams().TombstoneStream(context.Background(),
			streamName,
			event_streams.TombstoneRequestExpectedStreamRevisionNoStream{})
		require.Equal(t, errors.StreamDeletedErr, err.Code())
	})

	t.Run("Stream with events returns position", func(t *testing.T) {
		streamName := "stream_with_events"

		event := testCreateEvent()

		writeResult, err := client.EventStreams().AppendToStream(context.Background(),
			streamName,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			[]event_streams.ProposedEvent{event})
		require.NoError(t, err)

		currentRevision, _ := writeResult.GetCurrentRevision()
		tombstoneResult, err := client.EventStreams().TombstoneStream(context.Background(),
			streamName,
			event_streams.TombstoneRequestExpectedStreamRevision{Revision: currentRevision})
		require.NoError(t, err)

		tombstonePosition, isTombstonePosition := tombstoneResult.GetPosition()
		require.True(t, isTombstonePosition)
		writePosition, isWritePosition := writeResult.GetPosition()
		require.True(t, isWritePosition)

		require.True(t, tombstonePosition.GreaterThan(writePosition))
	})
}
