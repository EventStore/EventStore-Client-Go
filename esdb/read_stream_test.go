package esdb_test

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"io"
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/v4/esdb"
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

func ReadStreamTests(t *testing.T, emptyDBClient *esdb.Client) {
	t.Run("ReadStreamTests", func(t *testing.T) {
		t.Run("readStreamReturnsEOFAfterCompletion", readStreamReturnsEOFAfterCompletion(emptyDBClient))
		t.Run("readStreamNotFound", readStreamNotFound(emptyDBClient))
		t.Run("readStreamWithMaxAge", readStreamWithMaxAge(emptyDBClient))
		t.Run("readStreamWithCredentialsOverride", readStreamWithCredentialsOverride(emptyDBClient))
	})
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

func readStreamWithCredentialsOverride(db *esdb.Client) TestCall {
	return func(t *testing.T) {
		isInsecure := GetEnvOrDefault("EVENTSTORE_INSECURE", "true") == "true"

		if isInsecure {
			t.Skip()
		}

		streamName := NAME_GENERATOR.Generate()
		opts := esdb.AppendToStreamOptions{
			Authenticated: &esdb.Credentials{
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
