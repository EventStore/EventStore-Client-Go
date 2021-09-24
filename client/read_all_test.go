package client_test

import (
	"context"
	"testing"

	"github.com/pivonroll/EventStore-Client-Go/errors"

	"github.com/pivonroll/EventStore-Client-Go/systemmetadata"

	"github.com/pivonroll/EventStore-Client-Go/event_streams"
	"github.com/stretchr/testify/require"
)

func Test_ReadAll_Backwards(t *testing.T) {
	container := getEmptyDatabase()
	defer container.Close()
	client := createClientConnectedToContainer(container, t)
	defer func() {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}()

	streamId := "stream"
	events := testCreateEvents(20)
	events = append(events, testCreateEventsWithMetadata(2, 1_000_000)...)

	streamMetaData := event_streams.StreamMetadata{
		Acl: &event_streams.StreamAcl{
			ReadRoles: []string{string(systemmetadata.SystemRoleAll)},
		},
	}

	_, err := client.EventStreams().SetStreamMetadata(context.Background(),
		string(systemmetadata.AllStream),
		event_streams.AppendRequestExpectedStreamRevisionNoStream{},
		streamMetaData,
	)
	require.NoError(t, err)

	_, err = client.EventStreams().AppendToStream(context.Background(),
		streamId,
		event_streams.AppendRequestExpectedStreamRevisionNoStream{},
		events)
	require.NoError(t, err)

	t.Run("Return Empty If Reading From Start", func(t *testing.T) {
		readEvents, err := client.EventStreams().ReadAllEvents(context.Background(),
			event_streams.ReadRequestDirectionBackward,
			event_streams.ReadRequestOptionsAllStartPosition{},
			1,
			false)
		require.NoError(t, err)
		require.Len(t, readEvents, 0)
	})

	t.Run("Timeouts after context expires", func(t *testing.T) {
		ctx := context.Background()
		timeoutCtx, cancelFunc := context.WithTimeout(ctx, 0)
		defer cancelFunc()
		_, err = client.EventStreams().ReadAllEvents(timeoutCtx,
			event_streams.ReadRequestDirectionBackward,
			event_streams.ReadRequestOptionsAllStartPosition{},
			1,
			false)
		require.Equal(t, errors.DeadlineExceededErr, err.Code())
	})

	t.Run("Return Partial Slice If Not Enough Events", func(t *testing.T) {
		count := uint64(len(events)) * 2
		readEvents, err := client.EventStreams().ReadAllEvents(context.Background(),
			event_streams.ReadRequestDirectionBackward,
			event_streams.ReadRequestOptionsAllEndPosition{},
			count,
			false)
		require.NoError(t, err)
		require.Less(t, uint64(len(readEvents)), count)
	})

	t.Run("Return Events In Reversed Order Compared To Written", func(t *testing.T) {
		count := uint64(len(events))
		readEvents, err := client.EventStreams().ReadAllEvents(context.Background(),
			event_streams.ReadRequestDirectionBackward,
			event_streams.ReadRequestOptionsAllEndPosition{},
			count,
			false)
		require.NoError(t, err)
		require.Len(t, readEvents, int(count))

		readEventsToProposed := readEvents.Reverse().ToProposedEvents()
		require.Equal(t, events, readEventsToProposed)
	})

	t.Run("Return Single Event", func(t *testing.T) {
		readEvents, err := client.EventStreams().ReadAllEvents(context.Background(),
			event_streams.ReadRequestDirectionBackward,
			event_streams.ReadRequestOptionsAllEndPosition{},
			1,
			false)
		require.NoError(t, err)
		require.Len(t, readEvents, 1)

		readEventsToProposed := readEvents.ToProposedEvents()
		require.Equal(t, events[len(events)-1], readEventsToProposed[0])
	})

	t.Run("Max Count Is Respected", func(t *testing.T) {
		maxCount := len(events) / 2
		readEvents, err := client.EventStreams().ReadAllEvents(context.Background(),
			event_streams.ReadRequestDirectionBackward,
			event_streams.ReadRequestOptionsAllEndPosition{},
			uint64(maxCount),
			false)
		require.NoError(t, err)
		require.Len(t, readEvents, maxCount)
	})
}

func Test_ReadAll_Forwards(t *testing.T) {
	container := getEmptyDatabase()
	defer container.Close()
	client := createClientConnectedToContainer(container, t)
	defer func() {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}()

	streamId := "stream"
	events := testCreateEvents(20)
	events = append(events, testCreateEventsWithMetadata(2, 1_000_000)...)

	streamMetaData := event_streams.StreamMetadata{
		Acl: &event_streams.StreamAcl{
			ReadRoles: []string{string(systemmetadata.SystemRoleAll)},
		},
	}

	_, err := client.EventStreams().SetStreamMetadata(context.Background(),
		string(systemmetadata.AllStream),
		event_streams.AppendRequestExpectedStreamRevisionNoStream{},
		streamMetaData,
	)
	require.NoError(t, err)

	_, err = client.EventStreams().AppendToStream(context.Background(),
		streamId,
		event_streams.AppendRequestExpectedStreamRevisionNoStream{},
		events)
	require.NoError(t, err)

	t.Run("Return Empty If Reading From End", func(t *testing.T) {
		readEvents, err := client.EventStreams().ReadAllEvents(context.Background(),
			event_streams.ReadRequestDirectionForward,
			event_streams.ReadRequestOptionsAllEndPosition{},
			1,
			false)
		require.NoError(t, err)
		require.Len(t, readEvents, 0)
	})

	t.Run("Return Partial Slice If Not Enough Events", func(t *testing.T) {
		count := uint64(len(events)) * 2
		readEvents, err := client.EventStreams().ReadAllEvents(context.Background(),
			event_streams.ReadRequestDirectionForward,
			event_streams.ReadRequestOptionsAllStartPosition{},
			count,
			false)
		require.NoError(t, err)
		require.Less(t, uint64(len(readEvents)), count)
	})

	t.Run("Return Events In Correct Order Compared To Written", func(t *testing.T) {
		count := uint64(len(events)) * 2
		readEvents, err := client.EventStreams().ReadAllEvents(context.Background(),
			event_streams.ReadRequestDirectionForward,
			event_streams.ReadRequestOptionsAllStartPosition{},
			count,
			false)
		require.NoError(t, err)
		readEventsToProposed := readEvents.ToProposedEvents()
		require.Greater(t, len(readEventsToProposed), len(events))

		eventStart := len(readEventsToProposed) - len(events)
		require.Equal(t, readEventsToProposed[eventStart:], events)
	})

	t.Run("Return Single Event", func(t *testing.T) {
		readEvents, err := client.EventStreams().ReadAllEvents(context.Background(),
			event_streams.ReadRequestDirectionForward,
			event_streams.ReadRequestOptionsAllStartPosition{},
			1,
			false)
		require.NoError(t, err)
		require.Len(t, readEvents, 1)
	})

	t.Run("Max Count Is Respected", func(t *testing.T) {
		maxCount := len(events) / 2
		readEvents, err := client.EventStreams().ReadAllEvents(context.Background(),
			event_streams.ReadRequestDirectionForward,
			event_streams.ReadRequestOptionsAllStartPosition{},
			uint64(maxCount),
			false)
		require.NoError(t, err)
		require.Len(t, readEvents, maxCount)
	})
}
