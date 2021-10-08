package event_streams_with_prepopulated_database

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"testing"
	"time"

	"github.com/pivonroll/EventStore-Client-Go/event_streams"

	"github.com/stretchr/testify/require"
)

func Test_ReadAllEvents(t *testing.T) {
	client, closeFunc := initializeWithPrePopulatedDatabase(t)
	defer closeFunc()

	t.Run("ForwardsFromZeroPosition", func(t *testing.T) {
		eventsContent, err := ioutil.ReadFile(joinRootPathAndFilePath("resources/test/all-e0-e10.json"))
		require.NoError(t, err)

		var testEvents []TestEvent
		err = json.Unmarshal(eventsContent, &testEvents)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
		defer cancel()

		numberOfEventsToRead := 10
		numberOfEvents := uint64(numberOfEventsToRead)

		events, err := client.ReadEventsFromStreamAll(ctx,
			event_streams.ReadDirectionForward,
			event_streams.ReadPositionAllStart{}, numberOfEvents, true)
		require.NoError(t, err)

		for i := 0; i < numberOfEventsToRead; i++ {
			require.Equal(t, testEvents[i].Event.EventID, events[i].GetOriginalEvent().EventID)
			require.Equal(t, testEvents[i].Event.EventType, events[i].GetOriginalEvent().EventType)
			require.Equal(t, testEvents[i].Event.StreamID, events[i].GetOriginalEvent().StreamId)
			require.Equal(t, testEvents[i].Event.StreamRevision.Value, events[i].GetOriginalEvent().EventNumber)
			require.Equal(t, testEvents[i].Event.Created.Nanos, events[i].GetOriginalEvent().CreatedDateTime.Nanosecond())
			require.Equal(t, testEvents[i].Event.Created.Seconds, events[i].GetOriginalEvent().CreatedDateTime.Unix())
			require.Equal(t, testEvents[i].Event.Position.Commit, events[i].GetOriginalEvent().Position.Commit)
			require.Equal(t, testEvents[i].Event.Position.Prepare, events[i].GetOriginalEvent().Position.Prepare)
			require.EqualValues(t, testEvents[i].Event.ContentType, events[i].GetOriginalEvent().ContentType)
		}
	})

	t.Run("ForwardsFromNonZeroPosition", func(t *testing.T) {
		eventsContent, err := ioutil.ReadFile(joinRootPathAndFilePath("resources/test/all-c1788-p1788.json"))
		require.NoError(t, err)

		var testEvents []TestEvent
		err = json.Unmarshal(eventsContent, &testEvents)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
		defer cancel()

		numberOfEventsToRead := 10
		numberOfEvents := uint64(numberOfEventsToRead)

		events, err := client.ReadEventsFromStreamAll(ctx,
			event_streams.ReadDirectionForward,
			event_streams.ReadPositionAll{
				CommitPosition:  1788,
				PreparePosition: 1788,
			},
			numberOfEvents, true)
		require.NoError(t, err)

		for i := 0; i < numberOfEventsToRead; i++ {
			require.Equal(t, testEvents[i].Event.EventID, events[i].GetOriginalEvent().EventID)
			require.Equal(t, testEvents[i].Event.EventType, events[i].GetOriginalEvent().EventType)
			require.Equal(t, testEvents[i].Event.StreamID, events[i].GetOriginalEvent().StreamId)
			require.Equal(t, testEvents[i].Event.StreamRevision.Value, events[i].GetOriginalEvent().EventNumber)
			require.Equal(t, testEvents[i].Event.Created.Nanos, events[i].GetOriginalEvent().CreatedDateTime.Nanosecond())
			require.Equal(t, testEvents[i].Event.Created.Seconds, events[i].GetOriginalEvent().CreatedDateTime.Unix())
			require.Equal(t, testEvents[i].Event.Position.Commit, events[i].GetOriginalEvent().Position.Commit)
			require.Equal(t, testEvents[i].Event.Position.Prepare, events[i].GetOriginalEvent().Position.Prepare)
			require.EqualValues(t, testEvents[i].Event.ContentType, events[i].GetOriginalEvent().ContentType)
		}
	})

	t.Run("BackwardsFromZeroPosition", func(t *testing.T) {
		eventsContent, err := ioutil.ReadFile(joinRootPathAndFilePath("resources/test/all-back-e0-e10.json"))
		require.NoError(t, err)

		var testEvents []TestEvent
		err = json.Unmarshal(eventsContent, &testEvents)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
		defer cancel()

		numberOfEventsToRead := 10
		numberOfEvents := uint64(numberOfEventsToRead)

		events, err := client.ReadEventsFromStreamAll(ctx,
			event_streams.ReadDirectionBackward,
			event_streams.ReadPositionAllEnd{},
			numberOfEvents, true)
		require.NoError(t, err)

		for i := 0; i < numberOfEventsToRead; i++ {
			require.Equal(t, testEvents[i].Event.EventID, events[i].GetOriginalEvent().EventID)
			require.Equal(t, testEvents[i].Event.EventType, events[i].GetOriginalEvent().EventType)
			require.Equal(t, testEvents[i].Event.StreamID, events[i].GetOriginalEvent().StreamId)
			require.Equal(t, testEvents[i].Event.StreamRevision.Value, events[i].GetOriginalEvent().EventNumber)
			require.Equal(t, testEvents[i].Event.Created.Nanos, events[i].GetOriginalEvent().CreatedDateTime.Nanosecond())
			require.Equal(t, testEvents[i].Event.Created.Seconds, events[i].GetOriginalEvent().CreatedDateTime.Unix())
			require.Equal(t, testEvents[i].Event.Position.Commit, events[i].GetOriginalEvent().Position.Commit)
			require.Equal(t, testEvents[i].Event.Position.Prepare, events[i].GetOriginalEvent().Position.Prepare)
			require.EqualValues(t, testEvents[i].Event.ContentType, events[i].GetOriginalEvent().ContentType)
		}
	})

	t.Run("BackwardsFromNonZeroPosition", func(t *testing.T) {
		eventsContent, err := ioutil.ReadFile(joinRootPathAndFilePath("resources/test/all-back-c3386-p3386.json"))
		require.NoError(t, err)

		var testEvents []TestEvent
		err = json.Unmarshal(eventsContent, &testEvents)
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
		defer cancel()

		numberOfEventsToRead := 10
		numberOfEvents := uint64(numberOfEventsToRead)

		events, err := client.ReadEventsFromStreamAll(ctx,
			event_streams.ReadDirectionBackward,
			event_streams.ReadPositionAll{
				CommitPosition:  3386,
				PreparePosition: 3386,
			},
			numberOfEvents, true)
		require.NoError(t, err)

		for i := 0; i < numberOfEventsToRead; i++ {
			require.Equal(t, testEvents[i].Event.EventID, events[i].GetOriginalEvent().EventID)
			require.Equal(t, testEvents[i].Event.EventType, events[i].GetOriginalEvent().EventType)
			require.Equal(t, testEvents[i].Event.StreamID, events[i].GetOriginalEvent().StreamId)
			require.Equal(t, testEvents[i].Event.StreamRevision.Value, events[i].GetOriginalEvent().EventNumber)
			require.Equal(t, testEvents[i].Event.Created.Nanos, events[i].GetOriginalEvent().CreatedDateTime.Nanosecond())
			require.Equal(t, testEvents[i].Event.Created.Seconds, events[i].GetOriginalEvent().CreatedDateTime.Unix())
			require.Equal(t, testEvents[i].Event.Position.Commit, events[i].GetOriginalEvent().Position.Commit)
			require.Equal(t, testEvents[i].Event.Position.Prepare, events[i].GetOriginalEvent().Position.Prepare)
			require.EqualValues(t, testEvents[i].Event.ContentType, events[i].GetOriginalEvent().ContentType)
		}
	})
}
