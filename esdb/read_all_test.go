package esdb_test

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/v3/esdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func ReadAllTests(t *testing.T, populatedDBClient *esdb.Client) {
	t.Run("ReadAllTests", func(t *testing.T) {
		t.Run("readAllEventsForwardsFromZeroPosition(", readAllEventsForwardsFromZeroPosition(populatedDBClient))
		t.Run("readAllEventsForwardsFromNonZeroPosition", readAllEventsForwardsFromNonZeroPosition(populatedDBClient))
		t.Run("readAllEventsBackwardsFromZeroPosition", readAllEventsBackwardsFromZeroPosition(populatedDBClient))
		t.Run("readAllEventsBackwardsFromNonZeroPosition", readAllEventsBackwardsFromNonZeroPosition(populatedDBClient))
		t.Run("readAllEventsWithCredentialsOverride", readAllEventsWithCredentialOverride(populatedDBClient))
	})
}

func readAllEventsForwardsFromZeroPosition(db *esdb.Client) TestCall {
	return func(t *testing.T) {
		eventsContent, err := os.ReadFile("../resources/test/all-e0-e10.json")
		require.NoError(t, err)

		var testEvents []TestEvent
		err = json.Unmarshal(eventsContent, &testEvents)
		require.NoError(t, err)

		context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
		defer cancel()

		numberOfEventsToRead := 10
		numberOfEvents := uint64(numberOfEventsToRead)

		opts := esdb.ReadAllOptions{
			Direction:      esdb.Forwards,
			From:           esdb.Start{},
			ResolveLinkTos: true,
		}
		stream, err := db.ReadAll(context, opts, numberOfEvents)

		if err != nil {
			t.Fatalf("Unexpected failure %+v", err)
		}

		defer stream.Close()

		events, err := collectStreamEvents(stream)

		if err != nil {
			t.Fatalf("Unexpected failure %+v", err)
		}

		for i := 0; i < numberOfEventsToRead; i++ {
			assert.Equal(t, testEvents[i].Event.EventID, events[i].OriginalEvent().EventID)
			assert.Equal(t, testEvents[i].Event.EventType, events[i].OriginalEvent().EventType)
			assert.Equal(t, testEvents[i].Event.StreamID, events[i].OriginalEvent().StreamID)
			assert.Equal(t, testEvents[i].Event.StreamRevision.Value, events[i].OriginalEvent().EventNumber)
			assert.Equal(t, testEvents[i].Event.Created.Nanos, events[i].OriginalEvent().CreatedDate.Nanosecond())
			assert.Equal(t, testEvents[i].Event.Created.Seconds, events[i].OriginalEvent().CreatedDate.Unix())
			assert.Equal(t, testEvents[i].Event.Position.Commit, events[i].OriginalEvent().Position.Commit)
			assert.Equal(t, testEvents[i].Event.Position.Prepare, events[i].OriginalEvent().Position.Prepare)
			assert.Equal(t, testEvents[i].Event.ContentType, events[i].OriginalEvent().ContentType)
		}
	}
}

func readAllEventsForwardsFromNonZeroPosition(db *esdb.Client) TestCall {
	return func(t *testing.T) {
		eventsContent, err := ioutil.ReadFile("../resources/test/all-c1788-p1788.json")
		require.NoError(t, err)

		var testEvents []TestEvent
		err = json.Unmarshal(eventsContent, &testEvents)
		require.NoError(t, err)

		context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
		defer cancel()

		numberOfEventsToRead := 10
		numberOfEvents := uint64(numberOfEventsToRead)

		opts := esdb.ReadAllOptions{
			From:           esdb.Position{Commit: 1_788, Prepare: 1_788},
			ResolveLinkTos: true,
		}

		stream, err := db.ReadAll(context, opts, numberOfEvents)

		if err != nil {
			t.Fatalf("Unexpected failure %+v", err)
		}

		defer stream.Close()

		events, err := collectStreamEvents(stream)

		if err != nil {
			t.Fatalf("Unexpected failure %+v", err)
		}

		for i := 0; i < numberOfEventsToRead; i++ {
			assert.Equal(t, testEvents[i].Event.EventID, events[i].OriginalEvent().EventID)
			assert.Equal(t, testEvents[i].Event.EventType, events[i].OriginalEvent().EventType)
			assert.Equal(t, testEvents[i].Event.StreamID, events[i].OriginalEvent().StreamID)
			assert.Equal(t, testEvents[i].Event.StreamRevision.Value, events[i].OriginalEvent().EventNumber)
			assert.Equal(t, testEvents[i].Event.Created.Nanos, events[i].OriginalEvent().CreatedDate.Nanosecond())
			assert.Equal(t, testEvents[i].Event.Created.Seconds, events[i].OriginalEvent().CreatedDate.Unix())
			assert.Equal(t, testEvents[i].Event.Position.Commit, events[i].OriginalEvent().Position.Commit)
			assert.Equal(t, testEvents[i].Event.Position.Prepare, events[i].OriginalEvent().Position.Prepare)
			assert.Equal(t, testEvents[i].Event.ContentType, events[i].OriginalEvent().ContentType)
		}
	}
}

func readAllEventsBackwardsFromZeroPosition(db *esdb.Client) TestCall {
	return func(t *testing.T) {
		eventsContent, err := os.ReadFile("../resources/test/all-back-e0-e10.json")
		require.NoError(t, err)

		var testEvents []TestEvent
		err = json.Unmarshal(eventsContent, &testEvents)
		require.NoError(t, err)

		context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
		defer cancel()

		numberOfEventsToRead := 10
		numberOfEvents := uint64(numberOfEventsToRead)

		opts := esdb.ReadAllOptions{
			From:           esdb.End{},
			Direction:      esdb.Backwards,
			ResolveLinkTos: true,
		}

		// We read 30 more events in case the DB had pushed more config related events before the test begins.
		stream, err := db.ReadAll(context, opts, numberOfEvents+30)

		if err != nil {
			t.Fatalf("Unexpected failure %+v", err)
		}

		defer stream.Close()

		events, err := collectStreamEvents(stream)

		if err != nil {
			t.Fatalf("Unexpected failure %+v", err)
		}

		offset := 0

		// We remove potential events that were added by the server on startup, causing the expected event list to be
		// misaligned.
		for i := 0; i < numberOfEventsToRead; i++ {
			if events[i].OriginalEvent().CreatedDate.Year() == 2020 {
				break
			}

			offset += 1
		}

		for i := 0; i < numberOfEventsToRead; i++ {
			assert.Equal(t, testEvents[i].Event.EventID, events[i+offset].OriginalEvent().EventID)
			assert.Equal(t, testEvents[i].Event.EventType, events[i+offset].OriginalEvent().EventType)
			assert.Equal(t, testEvents[i].Event.StreamID, events[i+offset].OriginalEvent().StreamID)
			assert.Equal(t, testEvents[i].Event.StreamRevision.Value, events[i+offset].OriginalEvent().EventNumber)
			assert.Equal(t, testEvents[i].Event.Created.Nanos, events[i+offset].OriginalEvent().CreatedDate.Nanosecond())
			assert.Equal(t, testEvents[i].Event.Created.Seconds, events[i+offset].OriginalEvent().CreatedDate.Unix())
			assert.Equal(t, testEvents[i].Event.Position.Commit, events[i+offset].OriginalEvent().Position.Commit)
			assert.Equal(t, testEvents[i].Event.Position.Prepare, events[i+offset].OriginalEvent().Position.Prepare)
			assert.Equal(t, testEvents[i].Event.ContentType, events[i+offset].OriginalEvent().ContentType)
		}
	}
}

func readAllEventsBackwardsFromNonZeroPosition(db *esdb.Client) TestCall {
	return func(t *testing.T) {
		eventsContent, err := ioutil.ReadFile("../resources/test/all-back-c3386-p3386.json")
		require.NoError(t, err)

		var testEvents []TestEvent
		err = json.Unmarshal(eventsContent, &testEvents)
		require.NoError(t, err)

		context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
		defer cancel()

		numberOfEventsToRead := 10
		numberOfEvents := uint64(numberOfEventsToRead)

		opts := esdb.ReadAllOptions{
			From:           esdb.Position{Commit: 3_386, Prepare: 3_386},
			Direction:      esdb.Backwards,
			ResolveLinkTos: true,
		}

		stream, err := db.ReadAll(context, opts, numberOfEvents)

		if err != nil {
			t.Fatalf("Unexpected failure %+v", err)
		}

		defer stream.Close()

		events, err := collectStreamEvents(stream)

		if err != nil {
			t.Fatalf("Unexpected failure %+v", err)
		}

		assert.Equal(t, numberOfEvents, uint64(len(events)), "Expected the correct number of messages to be returned")

		for i := 0; i < numberOfEventsToRead; i++ {
			assert.Equal(t, testEvents[i].Event.EventID, events[i].OriginalEvent().EventID)
			assert.Equal(t, testEvents[i].Event.EventType, events[i].OriginalEvent().EventType)
			assert.Equal(t, testEvents[i].Event.StreamID, events[i].OriginalEvent().StreamID)
			assert.Equal(t, testEvents[i].Event.StreamRevision.Value, events[i].OriginalEvent().EventNumber)
			assert.Equal(t, testEvents[i].Event.Created.Nanos, events[i].OriginalEvent().CreatedDate.Nanosecond())
			assert.Equal(t, testEvents[i].Event.Created.Seconds, events[i].OriginalEvent().CreatedDate.Unix())
			assert.Equal(t, testEvents[i].Event.Position.Commit, events[i].OriginalEvent().Position.Commit)
			assert.Equal(t, testEvents[i].Event.Position.Prepare, events[i].OriginalEvent().Position.Prepare)
			assert.Equal(t, testEvents[i].Event.ContentType, events[i].OriginalEvent().ContentType)
		}
	}
}

func readAllEventsWithCredentialOverride(db *esdb.Client) TestCall {
	return func(t *testing.T) {
		eventsContent, err := ioutil.ReadFile("../resources/test/all-back-c3386-p3386.json")
		require.NoError(t, err)

		var testEvents []TestEvent
		err = json.Unmarshal(eventsContent, &testEvents)
		require.NoError(t, err)

		context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
		defer cancel()

		numberOfEventsToRead := 10
		numberOfEvents := uint64(numberOfEventsToRead)

		opts := esdb.ReadAllOptions{
			Authenticated: &esdb.Credentials{
				Login:    "admin",
				Password: "changeit",
			},
			From:           esdb.Position{Commit: 3_386, Prepare: 3_386},
			Direction:      esdb.Forwards,
			ResolveLinkTos: false,
		}

		stream, err := db.ReadAll(context, opts, numberOfEvents)

		if err != nil {
			t.Fatalf("Unexpected failure %+v", err)
		}

		defer stream.Close()

		// collect all events to see if no error occurs
		_, err = collectStreamEvents(stream)

		if err != nil {
			t.Fatalf("Unexpected failure %+v", err)
		}
	}
}
