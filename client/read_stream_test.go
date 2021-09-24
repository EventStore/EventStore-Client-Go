package client_test

import (
	"context"
	"testing"

	"github.com/pivonroll/EventStore-Client-Go/errors"

	uuid "github.com/gofrs/uuid"
	"github.com/pivonroll/EventStore-Client-Go/event_streams"
	"github.com/pivonroll/EventStore-Client-Go/systemmetadata"
	"github.com/stretchr/testify/require"
)

type Created struct {
	Seconds int64 `json:"seconds"`
	Nanos   int   `json:"nanos"`
}

type StreamRevision struct {
	Value uint64 `json:"value"`
}

type TestEvent struct {
	Event Event `json:"event"`
}

type Event struct {
	StreamID       string         `json:"streamId"`
	StreamRevision StreamRevision `json:"streamRevision"`
	EventID        uuid.UUID      `json:"eventId"`
	EventType      string         `json:"eventType"`
	EventData      []byte         `json:"eventData"`
	UserMetadata   []byte         `json:"userMetadata"`
	ContentType    string         `json:"contentType"`
	Position       Position       `json:"position"`
	Created        Created        `json:"created"`
}

func Test_Read_Forwards_Linked_Stream_Big_Count(t *testing.T) {
	container := getEmptyDatabase()
	defer container.Close()
	client := createClientConnectedToContainer(container, t)
	defer func() {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}()

	deletedStream := "deleted_stream"
	linkedStream := "linked_stream"

	_, err := client.EventStreams().AppendToStream(context.Background(),
		deletedStream,
		event_streams.AppendRequestExpectedStreamRevisionNoStream{},
		testCreateEvents(1))
	require.NoError(t, err)

	two := 2
	streamMetaData := event_streams.StreamMetadata{
		MaxCount: &two,
	}

	_, err = client.EventStreams().SetStreamMetadata(context.Background(),
		deletedStream,
		event_streams.AppendRequestExpectedStreamRevisionAny{},
		streamMetaData,
	)
	require.NoError(t, err)

	_, err = client.EventStreams().AppendToStream(context.Background(),
		deletedStream,
		event_streams.AppendRequestExpectedStreamRevisionNoStream{},
		testCreateEvents(1))
	require.NoError(t, err)

	_, err = client.EventStreams().AppendToStream(context.Background(),
		deletedStream,
		event_streams.AppendRequestExpectedStreamRevisionNoStream{},
		testCreateEvents(1))
	require.NoError(t, err)

	newUUID, _ := uuid.NewV4()
	linkedEvent := event_streams.ProposedEvent{
		EventID:      newUUID,
		EventType:    string(systemmetadata.SystemEventLinkTo),
		ContentType:  event_streams.ContentTypeOctetStream,
		Data:         []byte("0@" + deletedStream),
		UserMetadata: []byte{},
	}
	_, err = client.EventStreams().AppendToStream(context.Background(),
		linkedStream,
		event_streams.AppendRequestExpectedStreamRevisionNoStream{},
		[]event_streams.ProposedEvent{linkedEvent})
	require.NoError(t, err)

	readEvents, err := client.EventStreams().ReadStreamEvents(context.Background(),
		linkedStream,
		event_streams.ReadRequestDirectionForward,
		event_streams.ReadRequestOptionsStreamRevisionStart{},
		232323,
		true)
	require.NoError(t, err)
	require.Len(t, readEvents, 1)
}

func Test_Read_Forwards_Linked_Stream(t *testing.T) {
	container := getEmptyDatabase()
	defer container.Close()
	client := createClientConnectedToContainer(container, t)
	defer func() {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}()

	deletedStream := "deleted_stream"
	linkedStream := "linked_stream"

	_, err := client.EventStreams().AppendToStream(context.Background(),
		deletedStream,
		event_streams.AppendRequestExpectedStreamRevisionAny{},
		testCreateEvents(1))
	require.NoError(t, err)

	newUUID, _ := uuid.NewV4()
	linkedEvent := event_streams.ProposedEvent{
		EventID:      newUUID,
		EventType:    string(systemmetadata.SystemEventLinkTo),
		ContentType:  event_streams.ContentTypeOctetStream,
		Data:         []byte("0@" + deletedStream),
		UserMetadata: []byte{},
	}
	_, err = client.EventStreams().AppendToStream(context.Background(),
		linkedStream,
		event_streams.AppendRequestExpectedStreamRevisionNoStream{},
		[]event_streams.ProposedEvent{linkedEvent})
	require.NoError(t, err)

	_, err = client.EventStreams().DeleteStream(
		context.Background(),
		deletedStream, event_streams.DeleteRequestExpectedStreamRevisionAny{})
	require.NoError(t, err)

	readEvents, err := client.EventStreams().ReadStreamEvents(context.Background(),
		linkedStream,
		event_streams.ReadRequestDirectionForward,
		event_streams.ReadRequestOptionsStreamRevisionStart{},
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
	container := getEmptyDatabase()
	defer container.Close()
	client := createClientConnectedToContainer(container, t)
	defer func() {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}()

	deletedStream := "deleted_stream"
	linkedStream := "linked_stream"

	_, err := client.EventStreams().AppendToStream(context.Background(),
		deletedStream,
		event_streams.AppendRequestExpectedStreamRevisionAny{},
		testCreateEvents(1))
	require.NoError(t, err)

	newUUID, _ := uuid.NewV4()
	linkedEvent := event_streams.ProposedEvent{
		EventID:      newUUID,
		EventType:    string(systemmetadata.SystemEventLinkTo),
		ContentType:  event_streams.ContentTypeOctetStream,
		Data:         []byte("0@" + deletedStream),
		UserMetadata: []byte{},
	}
	_, err = client.EventStreams().AppendToStream(context.Background(),
		linkedStream,
		event_streams.AppendRequestExpectedStreamRevisionNoStream{},
		[]event_streams.ProposedEvent{linkedEvent})
	require.NoError(t, err)

	_, err = client.EventStreams().DeleteStream(
		context.Background(),
		deletedStream, event_streams.DeleteRequestExpectedStreamRevisionAny{})
	require.NoError(t, err)

	readEvents, err := client.EventStreams().ReadStreamEvents(context.Background(),
		linkedStream,
		event_streams.ReadRequestDirectionBackward,
		event_streams.ReadRequestOptionsStreamRevisionStart{},
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
	container := getEmptyDatabase()
	defer container.Close()
	client := createClientConnectedToContainer(container, t)
	defer func() {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}()

	t.Run("Stream Does Not Exist Throws", func(t *testing.T) {
		streamId := "stream_does_not_exist_throws"
		_, err := client.EventStreams().ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadRequestDirectionBackward,
			event_streams.ReadRequestOptionsStreamRevisionEnd{},
			1,
			false)
		require.Equal(t, errors.StreamNotFoundErr, err.Code())
	})

	t.Run("Deleted Stream", func(t *testing.T) {
		streamId := "deleted_stream"

		events := testCreateEvents(1)

		writeResult, err := client.EventStreams().AppendToStream(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		currentRevision, isCurrentRevision := writeResult.GetCurrentRevision()
		require.True(t, isCurrentRevision)
		require.Zero(t, currentRevision)

		_, err = client.EventStreams().DeleteStream(context.Background(),
			streamId,
			event_streams.DeleteRequestExpectedStreamRevision{Revision: currentRevision})
		require.NoError(t, err)

		_, err = client.EventStreams().ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadRequestDirectionBackward,
			event_streams.ReadRequestOptionsStreamRevisionEnd{},
			event_streams.ReadCountMax,
			false)
		require.Equal(t, errors.StreamNotFoundErr, err.Code())
	})

	t.Run("Tombstoned Stream StreamDeletedErr", func(t *testing.T) {
		streamId := "tombstoned_stream_err"

		_, err := client.EventStreams().TombstoneStream(context.Background(),
			streamId,
			event_streams.TombstoneRequestExpectedStreamRevisionNoStream{})
		require.NoError(t, err)

		_, err = client.EventStreams().ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadRequestDirectionBackward,
			event_streams.ReadRequestOptionsStreamRevisionEnd{},
			1,
			false)
		require.Equal(t, errors.StreamDeletedErr, err.Code())
	})

	t.Run("Returns Events In Reversed Order Small Events", func(t *testing.T) {
		streamId := "returns_events_in_reversed_order_small_events"
		events := testCreateEvents(10)

		_, err := client.EventStreams().AppendToStream(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		readEvents, err := client.EventStreams().ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadRequestDirectionBackward,
			event_streams.ReadRequestOptionsStreamRevisionEnd{},
			10,
			false)

		readEventsToProposed := readEvents.Reverse().ToProposedEvents()
		require.Equal(t, events, readEventsToProposed)
	})

	t.Run("Read Single Event From Arbitrary Position", func(t *testing.T) {
		streamId := "read_single_event_from_arbitrary_position"
		events := testCreateEvents(10)

		_, err := client.EventStreams().AppendToStream(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		readEvents, err := client.EventStreams().ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadRequestDirectionBackward,
			event_streams.ReadRequestOptionsStreamRevision{Revision: 7},
			1,
			false)

		require.Equal(t, events[7], readEvents[0].ToProposedEvent())
	})

	t.Run("Read Many Events From Arbitrary Position", func(t *testing.T) {
		streamId := "read_many_events_from_arbitrary_position"
		events := testCreateEvents(10)

		_, err := client.EventStreams().AppendToStream(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		readEvents, err := client.EventStreams().ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadRequestDirectionBackward,
			event_streams.ReadRequestOptionsStreamRevision{Revision: 3},
			2,
			false)

		require.Equal(t, events[2:4], readEvents.Reverse().ToProposedEvents())
	})

	t.Run("Read First Event", func(t *testing.T) {
		streamId := "read_first_event"
		events := testCreateEvents(10)

		_, err := client.EventStreams().AppendToStream(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		readEvents, err := client.EventStreams().ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadRequestDirectionBackward,
			event_streams.ReadRequestOptionsStreamRevisionStart{},
			1,
			false)

		require.Equal(t, events[0], readEvents[0].ToProposedEvent())
	})

	t.Run("Read Last Event", func(t *testing.T) {
		streamId := "read_last_event"
		events := testCreateEvents(10)

		_, err := client.EventStreams().AppendToStream(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		readEvents, err := client.EventStreams().ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadRequestDirectionBackward,
			event_streams.ReadRequestOptionsStreamRevisionEnd{},
			1,
			false)

		require.Equal(t, events[9], readEvents[0].ToProposedEvent())
	})

	t.Run("Max Count Is Respected", func(t *testing.T) {
		streamId := "max_count_is_respected"
		events := testCreateEvents(10)

		_, err := client.EventStreams().AppendToStream(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		readEvents, err := client.EventStreams().ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadRequestDirectionBackward,
			event_streams.ReadRequestOptionsStreamRevisionEnd{},
			5,
			false)

		require.Len(t, readEvents, 5)
	})

	t.Run("Deadline Exceeded if Context Times Out", func(t *testing.T) {
		streamId := "deadline_exceeded"

		ctx := context.Background()
		timeoutCtx, cancelFunc := context.WithTimeout(ctx, 0)
		defer cancelFunc()

		_, err := client.EventStreams().ReadStreamEvents(timeoutCtx,
			streamId,
			event_streams.ReadRequestDirectionBackward,
			event_streams.ReadRequestOptionsStreamRevisionEnd{},
			1,
			false)

		require.Equal(t, errors.DeadlineExceededErr, err.Code())
	})
}

func Test_Read_Forwards(t *testing.T) {
	container := getEmptyDatabase()
	defer container.Close()
	client := createClientConnectedToContainer(container, t)
	defer func() {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}()

	t.Run("Stream Does Not Exist Throws", func(t *testing.T) {
		streamId := "stream_does_not_exist_throws"
		_, err := client.EventStreams().ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadRequestDirectionForward,
			event_streams.ReadRequestOptionsStreamRevisionStart{},
			1,
			false)
		require.Equal(t, errors.StreamNotFoundErr, err.Code())
	})

	t.Run("Deleted Stream", func(t *testing.T) {
		streamId := "deleted_stream"

		events := testCreateEvents(1)

		writeResult, err := client.EventStreams().AppendToStream(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		currentRevision, isCurrentRevision := writeResult.GetCurrentRevision()
		require.True(t, isCurrentRevision)
		require.Zero(t, currentRevision)

		_, err = client.EventStreams().DeleteStream(context.Background(),
			streamId,
			event_streams.DeleteRequestExpectedStreamRevision{Revision: currentRevision})
		require.NoError(t, err)

		_, err = client.EventStreams().ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadRequestDirectionForward,
			event_streams.ReadRequestOptionsStreamRevisionStart{},
			event_streams.ReadCountMax,
			false)
		require.Equal(t, errors.StreamNotFoundErr, err.Code())
	})

	t.Run("Tombstoned Stream StreamDeletedErr", func(t *testing.T) {
		streamId := "tombstoned_stream"

		_, err := client.EventStreams().TombstoneStream(context.Background(),
			streamId,
			event_streams.TombstoneRequestExpectedStreamRevisionNoStream{})
		require.NoError(t, err)

		_, err = client.EventStreams().ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadRequestDirectionForward,
			event_streams.ReadRequestOptionsStreamRevisionStart{},
			1,
			false)
		require.Equal(t, errors.StreamDeletedErr, err.Code())
	})

	t.Run("Returns Events In Order Small Events", func(t *testing.T) {
		streamId := "returns_events_in_reversed_order_small_events"
		events := testCreateEvents(10)

		_, err := client.EventStreams().AppendToStream(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		readEvents, err := client.EventStreams().ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadRequestDirectionForward,
			event_streams.ReadRequestOptionsStreamRevisionStart{},
			10,
			false)

		readEventsToProposed := readEvents.ToProposedEvents()
		require.Equal(t, events, readEventsToProposed)
	})

	t.Run("Returns Events In Order Large Events", func(t *testing.T) {
		streamId := "returns_events_in_order_small_events"
		events := testCreateEventsWithMetadata(2, 1_000_000)

		_, err := client.EventStreams().AppendToStream(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		readEvents, err := client.EventStreams().ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadRequestDirectionForward,
			event_streams.ReadRequestOptionsStreamRevisionStart{},
			2,
			false)

		readEventsToProposed := readEvents.ToProposedEvents()
		require.Equal(t, events, readEventsToProposed)
	})

	t.Run("Read Single Event From Arbitrary Position", func(t *testing.T) {
		streamId := "read_single_event_from_arbitrary_position"
		events := testCreateEvents(10)

		_, err := client.EventStreams().AppendToStream(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		readEvents, err := client.EventStreams().ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadRequestDirectionForward,
			event_streams.ReadRequestOptionsStreamRevision{Revision: 7},
			1,
			false)

		require.Equal(t, events[7], readEvents[0].ToProposedEvent())
	})

	t.Run("Read Many Events From Arbitrary Position", func(t *testing.T) {
		streamId := "read_many_events_from_arbitrary_position"
		events := testCreateEvents(10)

		_, err := client.EventStreams().AppendToStream(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		readEvents, err := client.EventStreams().ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadRequestDirectionForward,
			event_streams.ReadRequestOptionsStreamRevision{Revision: 3},
			2,
			false)

		require.Equal(t, events[3:5], readEvents.ToProposedEvents())
	})

	t.Run("Read First Event", func(t *testing.T) {
		streamId := "read_first_event"
		events := testCreateEvents(10)

		_, err := client.EventStreams().AppendToStream(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		readEvents, err := client.EventStreams().ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadRequestDirectionForward,
			event_streams.ReadRequestOptionsStreamRevisionStart{},
			1,
			false)

		require.Equal(t, events[0], readEvents[0].ToProposedEvent())
	})

	t.Run("Max Count Is Respected", func(t *testing.T) {
		streamId := "max_count_is_respected"
		events := testCreateEvents(10)

		_, err := client.EventStreams().AppendToStream(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		readEvents, err := client.EventStreams().ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadRequestDirectionForward,
			event_streams.ReadRequestOptionsStreamRevisionStart{},
			5,
			false)

		require.Len(t, readEvents, 5)
	})

	t.Run("Read All Events By Default", func(t *testing.T) {
		streamId := "reads_all_events_by_default"
		events := testCreateEvents(200)

		_, err := client.EventStreams().AppendToStream(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		readEvents, err := client.EventStreams().ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadRequestDirectionForward,
			event_streams.ReadRequestOptionsStreamRevisionStart{},
			event_streams.ReadCountMax,
			false)

		require.Len(t, readEvents, 200)
	})

	t.Run("Deadline Exceeded if Context Times Out", func(t *testing.T) {
		streamId := "deadline_exceeded"

		ctx := context.Background()
		timeoutCtx, cancelFunc := context.WithTimeout(ctx, 0)
		defer cancelFunc()

		_, err := client.EventStreams().ReadStreamEvents(timeoutCtx,
			streamId,
			event_streams.ReadRequestDirectionForward,
			event_streams.ReadRequestOptionsStreamRevisionStart{},
			1,
			false)

		require.Equal(t, errors.DeadlineExceededErr, err.Code())
	})
}
