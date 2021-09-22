package client_test

import (
	"context"
	"testing"

	"github.com/EventStore/EventStore-Client-Go/errors"

	"github.com/EventStore/EventStore-Client-Go/systemmetadata"

	"github.com/EventStore/EventStore-Client-Go/event_streams"
	"github.com/stretchr/testify/require"
)

func Test_ReadAll_Backwards(t *testing.T) {
	container := GetEmptyDatabase()
	defer container.Close()
	client := CreateTestClient(container, t)
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

	_, err := client.SetStreamMetadata(context.Background(),
		string(systemmetadata.AllStream),
		event_streams.AppendRequestExpectedStreamRevisionNoStream{},
		streamMetaData,
	)
	require.NoError(t, err)

	_, err = client.AppendToStream(context.Background(),
		streamId,
		event_streams.AppendRequestExpectedStreamRevisionNoStream{},
		events)
	require.NoError(t, err)

	t.Run("Return Empty If Reading From Start", func(t *testing.T) {
		readEvents, err := client.ReadAllEvents(context.Background(),
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
		_, err = client.ReadAllEvents(timeoutCtx,
			event_streams.ReadRequestDirectionBackward,
			event_streams.ReadRequestOptionsAllStartPosition{},
			1,
			false)
		require.Equal(t, errors.DeadlineExceededErr, err.Code())
	})

	t.Run("Return Partial Slice If Not Enough Events", func(t *testing.T) {
		count := uint64(len(events)) * 2
		readEvents, err := client.ReadAllEvents(context.Background(),
			event_streams.ReadRequestDirectionBackward,
			event_streams.ReadRequestOptionsAllEndPosition{},
			count,
			false)
		require.NoError(t, err)
		require.Less(t, uint64(len(readEvents)), count)
	})

	t.Run("Return Events In Reversed Order Compared To Written", func(t *testing.T) {
		count := uint64(len(events))
		readEvents, err := client.ReadAllEvents(context.Background(),
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
		readEvents, err := client.ReadAllEvents(context.Background(),
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
		readEvents, err := client.ReadAllEvents(context.Background(),
			event_streams.ReadRequestDirectionBackward,
			event_streams.ReadRequestOptionsAllEndPosition{},
			uint64(maxCount),
			false)
		require.NoError(t, err)
		require.Len(t, readEvents, maxCount)
	})
}

func Test_ReadAll_Forwards(t *testing.T) {
	container := GetEmptyDatabase()
	defer container.Close()
	client := CreateTestClient(container, t)
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

	_, err := client.SetStreamMetadata(context.Background(),
		string(systemmetadata.AllStream),
		event_streams.AppendRequestExpectedStreamRevisionNoStream{},
		streamMetaData,
	)
	require.NoError(t, err)

	_, err = client.AppendToStream(context.Background(),
		streamId,
		event_streams.AppendRequestExpectedStreamRevisionNoStream{},
		events)
	require.NoError(t, err)

	t.Run("Return Empty If Reading From End", func(t *testing.T) {
		readEvents, err := client.ReadAllEvents(context.Background(),
			event_streams.ReadRequestDirectionForward,
			event_streams.ReadRequestOptionsAllEndPosition{},
			1,
			false)
		require.NoError(t, err)
		require.Len(t, readEvents, 0)
	})

	t.Run("Return Partial Slice If Not Enough Events", func(t *testing.T) {
		count := uint64(len(events)) * 2
		readEvents, err := client.ReadAllEvents(context.Background(),
			event_streams.ReadRequestDirectionForward,
			event_streams.ReadRequestOptionsAllStartPosition{},
			count,
			false)
		require.NoError(t, err)
		require.Less(t, uint64(len(readEvents)), count)
	})

	t.Run("Return Events In Correct Order Compared To Written", func(t *testing.T) {
		count := uint64(len(events)) * 2
		readEvents, err := client.ReadAllEvents(context.Background(),
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
		readEvents, err := client.ReadAllEvents(context.Background(),
			event_streams.ReadRequestDirectionForward,
			event_streams.ReadRequestOptionsAllStartPosition{},
			1,
			false)
		require.NoError(t, err)
		require.Len(t, readEvents, 1)
	})

	t.Run("Max Count Is Respected", func(t *testing.T) {
		maxCount := len(events) / 2
		readEvents, err := client.ReadAllEvents(context.Background(),
			event_streams.ReadRequestDirectionForward,
			event_streams.ReadRequestOptionsAllStartPosition{},
			uint64(maxCount),
			false)
		require.NoError(t, err)
		require.Len(t, readEvents, maxCount)
	})
}

//func TestReadAllEventsForwardsFromZeroPosition(t *testing.T) {
//	eventsContent, err := ioutil.ReadFile("../resources/test/all-e0-e10.json")
//	require.NoError(t, err)
//
//	var testEvents []TestEvent
//	err = json.Unmarshal(eventsContent, &testEvents)
//	require.NoError(t, err)
//
//	container := GetPrePopulatedDatabase()
//	defer container.Close()
//
//	client := CreateTestClient(container, t)
//	defer client.Close()
//
//	context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
//	defer cancel()
//
//	numberOfEventsToRead := 10
//	numberOfEvents := uint64(numberOfEventsToRead)
//
//	stream, err := client.ReadAllEvents_OLD(context, direction.Forwards, stream_position.Start{}, numberOfEvents, true)
//	if err != nil {
//		t.Fatalf("Unexpected failure %+v", err)
//	}
//
//	defer stream.Close()
//
//	events, err := collectStreamEvents(stream)
//	if err != nil {
//		t.Fatalf("Unexpected failure %+v", err)
//	}
//
//	for i := 0; i < numberOfEventsToRead; i++ {
//		assert.Equal(t, testEvents[i].Event.EventID, events[i].GetOriginalEvent().EventID)
//		assert.Equal(t, testEvents[i].Event.EventType, events[i].GetOriginalEvent().EventType)
//		assert.Equal(t, testEvents[i].Event.StreamID, events[i].GetOriginalEvent().StreamID)
//		assert.Equal(t, testEvents[i].Event.StreamRevision.Value, events[i].GetOriginalEvent().EventNumber)
//		assert.Equal(t, testEvents[i].Event.Created.Nanos, events[i].GetOriginalEvent().CreatedDate.Nanosecond())
//		assert.Equal(t, testEvents[i].Event.Created.Seconds, events[i].GetOriginalEvent().CreatedDate.Unix())
//		assert.Equal(t, testEvents[i].Event.Position.Commit, events[i].GetOriginalEvent().Position.Commit)
//		assert.Equal(t, testEvents[i].Event.Position.Prepare, events[i].GetOriginalEvent().Position.Prepare)
//		assert.Equal(t, testEvents[i].Event.ContentType, events[i].GetOriginalEvent().ContentType)
//	}
//}
//
//func TestReadAllEventsForwardsFromNonZeroPosition(t *testing.T) {
//	eventsContent, err := ioutil.ReadFile("../resources/test/all-c1788-p1788.json")
//	require.NoError(t, err)
//
//	var testEvents []TestEvent
//	err = json.Unmarshal(eventsContent, &testEvents)
//	require.NoError(t, err)
//
//	container := GetPrePopulatedDatabase()
//	defer container.Close()
//
//	client := CreateTestClient(container, t)
//	defer client.Close()
//
//	context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
//	defer cancel()
//
//	numberOfEventsToRead := 10
//	numberOfEvents := uint64(numberOfEventsToRead)
//
//	stream, err := client.ReadAllEvents_OLD(context, direction.Forwards, stream_position.Position{Value: position.Position{Commit: 1788, Prepare: 1788}}, numberOfEvents, true)
//	if err != nil {
//		t.Fatalf("Unexpected failure %+v", err)
//	}
//
//	defer stream.Close()
//
//	events, err := collectStreamEvents(stream)
//	if err != nil {
//		t.Fatalf("Unexpected failure %+v", err)
//	}
//
//	for i := 0; i < numberOfEventsToRead; i++ {
//		assert.Equal(t, testEvents[i].Event.EventID, events[i].GetOriginalEvent().EventID)
//		assert.Equal(t, testEvents[i].Event.EventType, events[i].GetOriginalEvent().EventType)
//		assert.Equal(t, testEvents[i].Event.StreamID, events[i].GetOriginalEvent().StreamID)
//		assert.Equal(t, testEvents[i].Event.StreamRevision.Value, events[i].GetOriginalEvent().EventNumber)
//		assert.Equal(t, testEvents[i].Event.Created.Nanos, events[i].GetOriginalEvent().CreatedDate.Nanosecond())
//		assert.Equal(t, testEvents[i].Event.Created.Seconds, events[i].GetOriginalEvent().CreatedDate.Unix())
//		assert.Equal(t, testEvents[i].Event.Position.Commit, events[i].GetOriginalEvent().Position.Commit)
//		assert.Equal(t, testEvents[i].Event.Position.Prepare, events[i].GetOriginalEvent().Position.Prepare)
//		assert.Equal(t, testEvents[i].Event.ContentType, events[i].GetOriginalEvent().ContentType)
//	}
//}
//
//func TestReadAllEventsBackwardsFromZeroPosition(t *testing.T) {
//	eventsContent, err := ioutil.ReadFile("../resources/test/all-back-e0-e10.json")
//	require.NoError(t, err)
//
//	var testEvents []TestEvent
//	err = json.Unmarshal(eventsContent, &testEvents)
//	require.NoError(t, err)
//
//	container := GetPrePopulatedDatabase()
//	defer container.Close()
//
//	client := CreateTestClient(container, t)
//	defer client.Close()
//
//	context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
//	defer cancel()
//
//	numberOfEventsToRead := 10
//	numberOfEvents := uint64(numberOfEventsToRead)
//
//	stream, err := client.ReadAllEvents_OLD(context, direction.Backwards, stream_position.End{}, numberOfEvents, true)
//	if err != nil {
//		t.Fatalf("Unexpected failure %+v", err)
//	}
//
//	defer stream.Close()
//
//	events, err := collectStreamEvents(stream)
//	if err != nil {
//		t.Fatalf("Unexpected failure %+v", err)
//	}
//
//	for i := 0; i < numberOfEventsToRead; i++ {
//		assert.Equal(t, testEvents[i].Event.EventID, events[i].GetOriginalEvent().EventID)
//		assert.Equal(t, testEvents[i].Event.EventType, events[i].GetOriginalEvent().EventType)
//		assert.Equal(t, testEvents[i].Event.StreamID, events[i].GetOriginalEvent().StreamID)
//		assert.Equal(t, testEvents[i].Event.StreamRevision.Value, events[i].GetOriginalEvent().EventNumber)
//		assert.Equal(t, testEvents[i].Event.Created.Nanos, events[i].GetOriginalEvent().CreatedDate.Nanosecond())
//		assert.Equal(t, testEvents[i].Event.Created.Seconds, events[i].GetOriginalEvent().CreatedDate.Unix())
//		assert.Equal(t, testEvents[i].Event.Position.Commit, events[i].GetOriginalEvent().Position.Commit)
//		assert.Equal(t, testEvents[i].Event.Position.Prepare, events[i].GetOriginalEvent().Position.Prepare)
//		assert.Equal(t, testEvents[i].Event.ContentType, events[i].GetOriginalEvent().ContentType)
//	}
//}
//
//func TestReadAllEventsBackwardsFromNonZeroPosition(t *testing.T) {
//	eventsContent, err := ioutil.ReadFile("../resources/test/all-back-c3386-p3386.json")
//	require.NoError(t, err)
//
//	var testEvents []TestEvent
//	err = json.Unmarshal(eventsContent, &testEvents)
//	require.NoError(t, err)
//
//	container := GetPrePopulatedDatabase()
//	defer container.Close()
//
//	client := CreateTestClient(container, t)
//	defer client.Close()
//
//	context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
//	defer cancel()
//
//	numberOfEventsToRead := 10
//	numberOfEvents := uint64(numberOfEventsToRead)
//
//	stream, err := client.ReadAllEvents_OLD(context, direction.Backwards, stream_position.Position{Value: position.Position{Commit: 3386, Prepare: 3386}}, numberOfEvents, true)
//	if err != nil {
//		t.Fatalf("Unexpected failure %+v", err)
//	}
//
//	defer stream.Close()
//
//	events, err := collectStreamEvents(stream)
//	if err != nil {
//		t.Fatalf("Unexpected failure %+v", err)
//	}
//
//	assert.Equal(t, numberOfEvents, uint64(len(events)), "Expected the correct number of messages to be returned")
//
//	for i := 0; i < numberOfEventsToRead; i++ {
//		assert.Equal(t, testEvents[i].Event.EventID, events[i].GetOriginalEvent().EventID)
//		assert.Equal(t, testEvents[i].Event.EventType, events[i].GetOriginalEvent().EventType)
//		assert.Equal(t, testEvents[i].Event.StreamID, events[i].GetOriginalEvent().StreamID)
//		assert.Equal(t, testEvents[i].Event.StreamRevision.Value, events[i].GetOriginalEvent().EventNumber)
//		assert.Equal(t, testEvents[i].Event.Created.Nanos, events[i].GetOriginalEvent().CreatedDate.Nanosecond())
//		assert.Equal(t, testEvents[i].Event.Created.Seconds, events[i].GetOriginalEvent().CreatedDate.Unix())
//		assert.Equal(t, testEvents[i].Event.Position.Commit, events[i].GetOriginalEvent().Position.Commit)
//		assert.Equal(t, testEvents[i].Event.Position.Prepare, events[i].GetOriginalEvent().Position.Prepare)
//		assert.Equal(t, testEvents[i].Event.ContentType, events[i].GetOriginalEvent().ContentType)
//	}
//}
