package read_from_stream

import (
	"context"
	"testing"

	"github.com/pivonroll/EventStore-Client-Go/errors"

	"github.com/google/uuid"
	"github.com/pivonroll/EventStore-Client-Go/event_streams"
	"github.com/pivonroll/EventStore-Client-Go/systemmetadata"
	"github.com/stretchr/testify/require"
)

func Test_Read_Forwards_Linked_Stream_Big_Count(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	deletedStream := "deleted_stream"
	linkedStream := "linked_stream"

	_, err := client.AppendToStream(context.Background(),
		deletedStream,
		event_streams.WriteStreamRevisionNoStream{},
		testCreateEvents(1))
	require.NoError(t, err)

	two := 2
	streamMetaData := event_streams.StreamMetadata{
		MaxCount: &two,
	}

	_, err = client.SetStreamMetadata(context.Background(),
		deletedStream,
		event_streams.WriteStreamRevisionAny{},
		streamMetaData,
	)
	require.NoError(t, err)

	_, err = client.AppendToStream(context.Background(),
		deletedStream,
		event_streams.WriteStreamRevisionAny{},
		testCreateEvents(1))
	require.NoError(t, err)

	_, err = client.AppendToStream(context.Background(),
		deletedStream,
		event_streams.WriteStreamRevisionAny{},
		testCreateEvents(1))
	require.NoError(t, err)

	newUUID, _ := uuid.NewRandom()
	linkedEvent := event_streams.ProposedEvent{
		EventId:      newUUID,
		EventType:    string(systemmetadata.SystemEventLinkTo),
		ContentType:  event_streams.ContentTypeOctetStream,
		Data:         []byte("0@" + deletedStream),
		UserMetadata: []byte{},
	}
	_, err = client.AppendToStream(context.Background(),
		linkedStream,
		event_streams.WriteStreamRevisionAny{},
		[]event_streams.ProposedEvent{linkedEvent})
	require.NoError(t, err)

	readEvents, err := client.ReadStreamEvents(context.Background(),
		linkedStream,
		event_streams.ReadDirectionForward,
		event_streams.ReadStreamRevisionStart{},
		232323,
		true)
	require.NoError(t, err)
	require.Len(t, readEvents, 1)
}

func Test_Read_Forwards_Linked_Stream(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	deletedStream := "deleted_stream"
	linkedStream := "linked_stream"

	_, err := client.AppendToStream(context.Background(),
		deletedStream,
		event_streams.WriteStreamRevisionAny{},
		testCreateEvents(1))
	require.NoError(t, err)

	newUUID, _ := uuid.NewRandom()
	linkedEvent := event_streams.ProposedEvent{
		EventId:      newUUID,
		EventType:    string(systemmetadata.SystemEventLinkTo),
		ContentType:  event_streams.ContentTypeOctetStream,
		Data:         []byte("0@" + deletedStream),
		UserMetadata: []byte{},
	}
	_, err = client.AppendToStream(context.Background(),
		linkedStream,
		event_streams.WriteStreamRevisionAny{},
		[]event_streams.ProposedEvent{linkedEvent})
	require.NoError(t, err)

	_, err = client.DeleteStream(
		context.Background(),
		deletedStream, event_streams.WriteStreamRevisionAny{})
	require.NoError(t, err)

	readEvents, err := client.ReadStreamEvents(context.Background(),
		linkedStream,
		event_streams.ReadDirectionForward,
		event_streams.ReadStreamRevisionStart{},
		1,
		true)

	require.NoError(t, err)

	t.Run("One Event Is Read", func(t *testing.T) {
		require.Len(t, readEvents, 1)
	})

	t.Run("Event Is Not Resolved", func(t *testing.T) {
		require.Nil(t, readEvents[0].Event)
	})

	t.Run("Link Event Is Included", func(t *testing.T) {
		require.NotNil(t, readEvents[0].Link)
	})
}

func Test_Read_Backwards_Linked_Stream(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	deletedStream := "deleted_stream"
	linkedStream := "linked_stream"

	_, err := client.AppendToStream(context.Background(),
		deletedStream,
		event_streams.WriteStreamRevisionAny{},
		testCreateEvents(1))
	require.NoError(t, err)

	newUUID, _ := uuid.NewRandom()
	linkedEvent := event_streams.ProposedEvent{
		EventId:      newUUID,
		EventType:    string(systemmetadata.SystemEventLinkTo),
		ContentType:  event_streams.ContentTypeOctetStream,
		Data:         []byte("0@" + deletedStream),
		UserMetadata: []byte{},
	}
	_, err = client.AppendToStream(context.Background(),
		linkedStream,
		event_streams.WriteStreamRevisionAny{},
		[]event_streams.ProposedEvent{linkedEvent})
	require.NoError(t, err)

	_, err = client.DeleteStream(
		context.Background(),
		deletedStream, event_streams.WriteStreamRevisionAny{})
	require.NoError(t, err)

	readEvents, err := client.ReadStreamEvents(context.Background(),
		linkedStream,
		event_streams.ReadDirectionBackward,
		event_streams.ReadStreamRevisionStart{},
		1,
		true)

	require.NoError(t, err)

	t.Run("One Event Is Read", func(t *testing.T) {
		require.Len(t, readEvents, 1)
	})

	t.Run("Event Is Not Resolved", func(t *testing.T) {
		require.Nil(t, readEvents[0].Event)
	})

	t.Run("Link Event Is Included", func(t *testing.T) {
		require.NotNil(t, readEvents[0].Link)
	})
}

func Test_Read_Backwards(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	t.Run("Stream Does Not Exist Throws", func(t *testing.T) {
		streamId := "stream_does_not_exist_throws"
		_, err := client.ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadDirectionBackward,
			event_streams.ReadStreamRevisionEnd{},
			1,
			false)
		require.Equal(t, errors.StreamNotFoundErr, err.Code())
	})

	t.Run("Deleted Stream", func(t *testing.T) {
		streamId := "deleted_stream"

		events := testCreateEvents(1)

		writeResult, err := client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionNoStream{},
			events)
		require.NoError(t, err)
		require.Zero(t, writeResult.GetCurrentRevision())

		_, err = client.DeleteStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevision{Revision: writeResult.GetCurrentRevision()})
		require.NoError(t, err)

		_, err = client.ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadDirectionBackward,
			event_streams.ReadStreamRevisionEnd{},
			event_streams.ReadCountMax,
			false)
		require.Equal(t, errors.StreamNotFoundErr, err.Code())
	})

	t.Run(`After Putting a Tombstone on Stream 
			Read Returns StreamDeletedErr`, func(t *testing.T) {
		streamId := "tombstoned_stream_err"

		_, err := client.TombstoneStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionNoStream{})
		require.NoError(t, err)

		_, err = client.ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadDirectionBackward,
			event_streams.ReadStreamRevisionEnd{},
			1,
			false)
		require.Equal(t, errors.StreamDeletedErr, err.Code())
	})

	t.Run("Returns Events In Reversed Order - Small Events", func(t *testing.T) {
		streamId := "returns_events_in_reversed_order_small_events"
		events := testCreateEvents(10)

		_, err := client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		readEvents, err := client.ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadDirectionBackward,
			event_streams.ReadStreamRevisionEnd{},
			10,
			false)

		readEventsToProposed := readEvents.Reverse().ToProposedEvents()
		require.Equal(t, events, readEventsToProposed)
	})

	t.Run("Read Single Event From Arbitrary Position", func(t *testing.T) {
		streamId := "read_single_event_from_arbitrary_position"
		events := testCreateEvents(10)

		_, err := client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		readEvents, err := client.ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadDirectionBackward,
			event_streams.ReadStreamRevision{Revision: 7},
			1,
			false)

		require.Equal(t, events[7], readEvents[0].ToProposedEvent())
	})

	t.Run("Read Many Events From Arbitrary Position", func(t *testing.T) {
		streamId := "read_many_events_from_arbitrary_position"
		events := testCreateEvents(10)

		_, err := client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		readEvents, err := client.ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadDirectionBackward,
			event_streams.ReadStreamRevision{Revision: 3},
			2,
			false)

		require.Equal(t, events[2:4], readEvents.Reverse().ToProposedEvents())
	})

	t.Run("Read First Event", func(t *testing.T) {
		streamId := "read_first_event"
		events := testCreateEvents(10)

		_, err := client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		readEvents, err := client.ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadDirectionBackward,
			event_streams.ReadStreamRevisionStart{},
			1,
			false)

		require.Equal(t, events[0], readEvents[0].ToProposedEvent())
	})

	t.Run("Read Last Event", func(t *testing.T) {
		streamId := "read_last_event"
		events := testCreateEvents(10)

		_, err := client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		readEvents, err := client.ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadDirectionBackward,
			event_streams.ReadStreamRevisionEnd{},
			1,
			false)

		require.Equal(t, events[9], readEvents[0].ToProposedEvent())
	})

	t.Run("Max Count Is Respected", func(t *testing.T) {
		streamId := "max_count_is_respected"
		events := testCreateEvents(10)

		_, err := client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		readEvents, err := client.ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadDirectionBackward,
			event_streams.ReadStreamRevisionEnd{},
			5,
			false)

		require.Len(t, readEvents, 5)
	})

	t.Run("Deadline Exceeded if Context Times Out", func(t *testing.T) {
		streamId := "deadline_exceeded"

		ctx := context.Background()
		timeoutCtx, cancelFunc := context.WithTimeout(ctx, 0)
		defer cancelFunc()

		_, err := client.ReadStreamEvents(timeoutCtx,
			streamId,
			event_streams.ReadDirectionBackward,
			event_streams.ReadStreamRevisionEnd{},
			1,
			false)

		require.Equal(t, errors.DeadlineExceededErr, err.Code())
	})
}

func Test_Read_Forwards(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	t.Run("Stream Does Not Exist Throws", func(t *testing.T) {
		streamId := "stream_does_not_exist_throws"
		_, err := client.ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadDirectionForward,
			event_streams.ReadStreamRevisionStart{},
			1,
			false)
		require.Equal(t, errors.StreamNotFoundErr, err.Code())
	})

	t.Run("Deleted Stream", func(t *testing.T) {
		streamId := "deleted_stream"

		events := testCreateEvents(1)

		writeResult, err := client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		require.Zero(t, writeResult.GetCurrentRevision())

		_, err = client.DeleteStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevision{Revision: writeResult.GetCurrentRevision()})
		require.NoError(t, err)

		_, err = client.ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadDirectionForward,
			event_streams.ReadStreamRevisionStart{},
			event_streams.ReadCountMax,
			false)
		require.Equal(t, errors.StreamNotFoundErr, err.Code())
	})

	t.Run("Tombstoned Stream StreamDeletedErr", func(t *testing.T) {
		streamId := "tombstoned_stream"

		_, err := client.TombstoneStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionNoStream{})
		require.NoError(t, err)

		_, err = client.ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadDirectionForward,
			event_streams.ReadStreamRevisionStart{},
			1,
			false)
		require.Equal(t, errors.StreamDeletedErr, err.Code())
	})

	t.Run("Returns Events In Order Small Events", func(t *testing.T) {
		streamId := "returns_events_in_reversed_order_small_events"
		events := testCreateEvents(10)

		_, err := client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		readEvents, err := client.ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadDirectionForward,
			event_streams.ReadStreamRevisionStart{},
			10,
			false)

		readEventsToProposed := readEvents.ToProposedEvents()
		require.Equal(t, events, readEventsToProposed)
	})

	t.Run("Returns Events In Order Large Events", func(t *testing.T) {
		streamId := "returns_events_in_order_small_events"
		events := testCreateEventsWithMetadata(2, 1_000_000)

		_, err := client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		readEvents, err := client.ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadDirectionForward,
			event_streams.ReadStreamRevisionStart{},
			2,
			false)

		readEventsToProposed := readEvents.ToProposedEvents()
		require.Equal(t, events, readEventsToProposed)
	})

	t.Run("Read Single Event From Arbitrary Position", func(t *testing.T) {
		streamId := "read_single_event_from_arbitrary_position"
		events := testCreateEvents(10)

		_, err := client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		readEvents, err := client.ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadDirectionForward,
			event_streams.ReadStreamRevision{Revision: 7},
			1,
			false)

		require.Equal(t, events[7], readEvents[0].ToProposedEvent())
	})

	t.Run("Read Many Events From Arbitrary Position", func(t *testing.T) {
		streamId := "read_many_events_from_arbitrary_position"
		events := testCreateEvents(10)

		_, err := client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		readEvents, err := client.ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadDirectionForward,
			event_streams.ReadStreamRevision{Revision: 3},
			2,
			false)

		require.Equal(t, events[3:5], readEvents.ToProposedEvents())
	})

	t.Run("Read First Event", func(t *testing.T) {
		streamId := "read_first_event"
		events := testCreateEvents(10)

		_, err := client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		readEvents, err := client.ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadDirectionForward,
			event_streams.ReadStreamRevisionStart{},
			1,
			false)

		require.Equal(t, events[0], readEvents[0].ToProposedEvent())
	})

	t.Run("Max Count Is Respected", func(t *testing.T) {
		streamId := "max_count_is_respected"
		events := testCreateEvents(10)

		_, err := client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		readEvents, err := client.ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadDirectionForward,
			event_streams.ReadStreamRevisionStart{},
			5,
			false)

		require.Len(t, readEvents, 5)
	})

	t.Run("Read All Events By Default", func(t *testing.T) {
		streamId := "reads_all_events_by_default"
		events := testCreateEvents(200)

		_, err := client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		readEvents, err := client.ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadDirectionForward,
			event_streams.ReadStreamRevisionStart{},
			event_streams.ReadCountMax,
			false)

		require.Len(t, readEvents, 200)
	})

	t.Run("Deadline Exceeded if Context Times Out", func(t *testing.T) {
		streamId := "deadline_exceeded"

		ctx := context.Background()
		timeoutCtx, cancelFunc := context.WithTimeout(ctx, 0)
		defer cancelFunc()

		_, err := client.ReadStreamEvents(timeoutCtx,
			streamId,
			event_streams.ReadDirectionForward,
			event_streams.ReadStreamRevisionStart{},
			1,
			false)

		require.Equal(t, errors.DeadlineExceededErr, err.Code())
	})
}

func Test_Read_WithIncorrectCredentials(t *testing.T) {
	client, closeFunc := initializeContainerAndClientWithCredentials(t,
		"wrong_user_name", "wrong_password", nil)
	defer closeFunc()

	streamId := "stream_does_not_exist_throws"
	_, err := client.ReadStreamEvents(context.Background(),
		streamId,
		event_streams.ReadDirectionForward,
		event_streams.ReadStreamRevisionStart{},
		1,
		false)
	require.Equal(t, errors.UnauthenticatedErr, err.Code())
}
