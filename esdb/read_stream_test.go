package esdb_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/v2/esdb"
	uuid "github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
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

func ReadStreamTests(t *testing.T, emptyDBClient *esdb.Client, populatedDBClient *esdb.Client) {
	t.Run("ReadStreamTests", func(t *testing.T) {
		t.Run("readStreamEventsForwardsFromZeroPosition", readStreamEventsForwardsFromZeroPosition(populatedDBClient))
		t.Run("readStreamEventsBackwardsFromEndPosition", readStreamEventsBackwardsFromEndPosition(populatedDBClient))
		t.Run("readStreamReturnsEOFAfterCompletion", readStreamReturnsEOFAfterCompletion(emptyDBClient))
		t.Run("readStreamNotFound", readStreamNotFound(emptyDBClient))
		t.Run("readStreamWithMaxAge", readStreamWithMaxAge(emptyDBClient))
	})
}

func readStreamEventsForwardsFromZeroPosition(db *esdb.Client) TestCall {
	return func(t *testing.T) {
		if db == nil {
			t.Skip()
		}

		eventsContent, err := ioutil.ReadFile("../resources/test/dataset20M-1800-e0-e10.json")
		require.NoError(t, err)

		var testEvents []TestEvent
		err = json.Unmarshal(eventsContent, &testEvents)
		require.NoError(t, err)

		context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
		defer cancel()

		numberOfEventsToRead := 10
		numberOfEvents := uint64(numberOfEventsToRead)

		streamId := "dataset20M-1800"

		opts := esdb.ReadStreamOptions{
			Direction:      esdb.Forwards,
			ResolveLinkTos: true,
		}

		stream, err := db.ReadStream(context, streamId, opts, numberOfEvents)

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

func readStreamEventsBackwardsFromEndPosition(db *esdb.Client) TestCall {
	return func(t *testing.T) {
		if db == nil {
			t.Skip()
		}

		eventsContent, err := ioutil.ReadFile("../resources/test/dataset20M-1800-e1999-e1990.json")
		require.NoError(t, err)

		var testEvents []TestEvent
		err = json.Unmarshal(eventsContent, &testEvents)

		require.NoError(t, err)

		context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
		defer cancel()

		numberOfEventsToRead := 10
		numberOfEvents := uint64(numberOfEventsToRead)

		streamId := "dataset20M-1800"
		opts := esdb.ReadStreamOptions{
			Direction:      esdb.Backwards,
			From:           esdb.End{},
			ResolveLinkTos: true,
		}

		stream, err := db.ReadStream(context, streamId, opts, numberOfEvents)

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

func readStreamReturnsEOFAfterCompletion(db *esdb.Client) TestCall {
	return func(t *testing.T) {
		proposedEvents := []esdb.EventData{}

		for i := 1; i <= 10; i++ {
			proposedEvents = append(proposedEvents, createTestEvent())
		}

		opts := esdb.AppendToStreamOptions{
			ExpectedRevision: esdb.NoStream{},
		}

		streamID := NAME_GENERATOR.Generate()

		_, err := db.AppendToStream(context.Background(), streamID, opts, proposedEvents...)
		require.NoError(t, err)

		stream, err := db.ReadStream(context.Background(), streamID, esdb.ReadStreamOptions{}, 1_024)

		require.NoError(t, err)
		defer stream.Close()
		_, err = collectStreamEvents(stream)
		require.NoError(t, err)

		_, err = stream.Recv()
		require.Error(t, err)
		require.True(t, err == io.EOF)
	}
}

func readStreamNotFound(db *esdb.Client) TestCall {
	return func(t *testing.T) {
		stream, err := db.ReadStream(context.Background(), NAME_GENERATOR.Generate(), esdb.ReadStreamOptions{}, 1)

		require.NoError(t, err)
		defer stream.Close()

		evt, err := stream.Recv()
		require.Nil(t, evt)

		esdbErr, ok := esdb.FromError(err)

		require.False(t, ok)
		require.Equal(t, esdbErr.Code(), esdb.ErrorCodeResourceNotFound)
	}
}

func readStreamWithMaxAge(db *esdb.Client) TestCall {
	return func(t *testing.T) {
		streamName := NAME_GENERATOR.Generate()
		_, err := db.AppendToStream(context.Background(), streamName, esdb.AppendToStreamOptions{}, createTestEvent())

		assert.NoError(t, err)

		metadata := esdb.StreamMetadata{}
		metadata.SetMaxAge(time.Second)

		_, err = db.SetStreamMetadata(context.Background(), streamName, esdb.AppendToStreamOptions{}, metadata)

		assert.NoError(t, err)

		time.Sleep(2 * time.Second)

		stream, err := db.ReadStream(context.Background(), streamName, esdb.ReadStreamOptions{}, 10)
		require.NoError(t, err)
		defer stream.Close()

		evt, err := stream.Recv()
		require.Nil(t, evt)
		require.Error(t, err)
		require.True(t, errors.Is(err, io.EOF))
	}
}
