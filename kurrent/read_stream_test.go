package kurrent_test

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"io"
	"io/ioutil"
	"math"
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/v4/kurrent"
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

func ReadStreamTests(t *testing.T, emptyDBClient *kurrent.Client, populatedDBClient *kurrent.Client) {
	t.Run("ReadStreamTests", func(t *testing.T) {
		t.Run("readStreamEventsForwardsFromZeroPosition", readStreamEventsForwardsFromZeroPosition(populatedDBClient))
		t.Run("readStreamEventsBackwardsFromEndPosition", readStreamEventsBackwardsFromEndPosition(populatedDBClient))
		t.Run("readStreamReturnsEOFAfterCompletion", readStreamReturnsEOFAfterCompletion(emptyDBClient))
		t.Run("readStreamNotFound", readStreamNotFound(emptyDBClient))
		t.Run("readStreamWithMaxAge", readStreamWithMaxAge(emptyDBClient))
		t.Run("readStreamWithCredentialsOverride", readStreamWithCredentialsOverride(emptyDBClient))
	})
}

func readStreamEventsForwardsFromZeroPosition(db *kurrent.Client) TestCall {
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

		opts := kurrent.ReadStreamOptions{
			Direction:      kurrent.Forwards,
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

		serverVersion, err := db.GetServerVersion()
		if err != nil {
			t.Fatalf("Failed to retrieve server version %+v", err)
		}

		isAtLeast22_6 := serverVersion.Major == 22 && serverVersion.Minor >= 6 || serverVersion.Major > 22

		for i := 0; i < numberOfEventsToRead; i++ {
			assert.Equal(t, testEvents[i].Event.EventID, events[i].OriginalEvent().EventID)
			assert.Equal(t, testEvents[i].Event.EventType, events[i].OriginalEvent().EventType)
			assert.Equal(t, testEvents[i].Event.StreamID, events[i].OriginalEvent().StreamID)
			assert.Equal(t, testEvents[i].Event.StreamRevision.Value, events[i].OriginalEvent().EventNumber)
			assert.Equal(t, testEvents[i].Event.Created.Nanos, events[i].OriginalEvent().CreatedDate.Nanosecond())
			assert.Equal(t, testEvents[i].Event.Created.Seconds, events[i].OriginalEvent().CreatedDate.Unix())
			if isAtLeast22_6 {
				assert.Equal(t, testEvents[i].Event.Position.Commit, events[i].OriginalEvent().Position.Commit)
				assert.Equal(t, testEvents[i].Event.Position.Prepare, events[i].OriginalEvent().Position.Prepare)
			} else {
				assert.Equal(t, uint64(math.MaxUint64), events[i].OriginalEvent().Position.Commit)
				assert.Equal(t, uint64(math.MaxUint64), events[i].OriginalEvent().Position.Prepare)
			}

			assert.Equal(t, testEvents[i].Event.ContentType, events[i].OriginalEvent().ContentType)
		}
	}
}

func readStreamEventsBackwardsFromEndPosition(db *kurrent.Client) TestCall {
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
		opts := kurrent.ReadStreamOptions{
			Direction:      kurrent.Backwards,
			From:           kurrent.End{},
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

		serverVersion, err := db.GetServerVersion()
		if err != nil {
			t.Fatalf("Failed to retrieve server version %+v", err)
		}

		isAtLeast22_6 := serverVersion.Major == 22 && serverVersion.Minor >= 6 || serverVersion.Major > 22

		for i := 0; i < numberOfEventsToRead; i++ {
			assert.Equal(t, testEvents[i].Event.EventID, events[i].OriginalEvent().EventID)
			assert.Equal(t, testEvents[i].Event.EventType, events[i].OriginalEvent().EventType)
			assert.Equal(t, testEvents[i].Event.StreamID, events[i].OriginalEvent().StreamID)
			assert.Equal(t, testEvents[i].Event.StreamRevision.Value, events[i].OriginalEvent().EventNumber)
			assert.Equal(t, testEvents[i].Event.Created.Nanos, events[i].OriginalEvent().CreatedDate.Nanosecond())
			assert.Equal(t, testEvents[i].Event.Created.Seconds, events[i].OriginalEvent().CreatedDate.Unix())
			if isAtLeast22_6 {
				assert.Equal(t, testEvents[i].Event.Position.Commit, events[i].OriginalEvent().Position.Commit)
				assert.Equal(t, testEvents[i].Event.Position.Prepare, events[i].OriginalEvent().Position.Prepare)
			} else {
				assert.Equal(t, uint64(math.MaxUint64), events[i].OriginalEvent().Position.Commit)
				assert.Equal(t, uint64(math.MaxUint64), events[i].OriginalEvent().Position.Prepare)
			}
			assert.Equal(t, testEvents[i].Event.ContentType, events[i].OriginalEvent().ContentType)
		}
	}
}

func readStreamReturnsEOFAfterCompletion(db *kurrent.Client) TestCall {
	return func(t *testing.T) {
		proposedEvents := []kurrent.EventData{}

		for i := 1; i <= 10; i++ {
			proposedEvents = append(proposedEvents, createTestEvent())
		}

		opts := kurrent.AppendToStreamOptions{
			ExpectedRevision: kurrent.NoStream{},
		}

		streamID := NAME_GENERATOR.Generate()

		_, err := db.AppendToStream(context.Background(), streamID, opts, proposedEvents...)
		require.NoError(t, err)

		stream, err := db.ReadStream(context.Background(), streamID, kurrent.ReadStreamOptions{}, 1_024)

		require.NoError(t, err)
		defer stream.Close()
		_, err = collectStreamEvents(stream)
		require.NoError(t, err)

		_, err = stream.Recv()
		require.Error(t, err)
		require.True(t, err == io.EOF)
	}
}

func readStreamNotFound(db *kurrent.Client) TestCall {
	return func(t *testing.T) {
		stream, err := db.ReadStream(context.Background(), NAME_GENERATOR.Generate(), kurrent.ReadStreamOptions{}, 1)

		require.NoError(t, err)
		defer stream.Close()

		evt, err := stream.Recv()
		require.Nil(t, evt)

		esdbErr, ok := kurrent.FromError(err)

		require.False(t, ok)
		require.Equal(t, esdbErr.Code(), kurrent.ErrorCodeResourceNotFound)
	}
}

func readStreamWithMaxAge(db *kurrent.Client) TestCall {
	return func(t *testing.T) {
		streamName := NAME_GENERATOR.Generate()
		_, err := db.AppendToStream(context.Background(), streamName, kurrent.AppendToStreamOptions{}, createTestEvent())

		assert.NoError(t, err)

		metadata := kurrent.StreamMetadata{}
		metadata.SetMaxAge(time.Second)

		_, err = db.SetStreamMetadata(context.Background(), streamName, kurrent.AppendToStreamOptions{}, metadata)

		assert.NoError(t, err)

		time.Sleep(2 * time.Second)

		stream, err := db.ReadStream(context.Background(), streamName, kurrent.ReadStreamOptions{}, 10)
		require.NoError(t, err)
		defer stream.Close()

		evt, err := stream.Recv()
		require.Nil(t, evt)
		require.Error(t, err)
		require.True(t, errors.Is(err, io.EOF))
	}
}

func readStreamWithCredentialsOverride(db *kurrent.Client) TestCall {
	return func(t *testing.T) {
		isInsecure := GetEnvOrDefault("EVENTSTORE_INSECURE", "true") == "true"

		if isInsecure {
			t.Skip()
		}

		streamName := NAME_GENERATOR.Generate()
		opts := kurrent.AppendToStreamOptions{
			Authenticated: &kurrent.Credentials{
				Login:    "admin",
				Password: "changeit",
			},
		}
		_, err := db.AppendToStream(context.Background(), streamName, opts, createTestEvent())

		assert.NoError(t, err)

		streamName = NAME_GENERATOR.Generate()
		opts.Authenticated.Password = "invalid"
		_, err = db.AppendToStream(context.Background(), streamName, opts, createTestEvent())

		assert.Error(t, err)
	}
}
