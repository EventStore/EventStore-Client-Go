package esdb_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"sync"
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/esdb"
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

func TestReadStreamEventsForwardsFromZeroPosition(t *testing.T) {
	eventsContent, err := ioutil.ReadFile("../resources/test/dataset20M-1800-e0-e10.json")
	require.NoError(t, err)

	var testEvents []TestEvent
	err = json.Unmarshal(eventsContent, &testEvents)
	require.NoError(t, err)

	container := GetPrePopulatedDatabase()
	defer container.Close()

	db := CreateTestClient(container, t)
	defer db.Close()

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

func TestReadStreamEventsBackwardsFromEndPosition(t *testing.T) {
	eventsContent, err := ioutil.ReadFile("../resources/test/dataset20M-1800-e1999-e1990.json")
	require.NoError(t, err)

	var testEvents []TestEvent
	err = json.Unmarshal(eventsContent, &testEvents)

	require.NoError(t, err)
	container := GetPrePopulatedDatabase()
	defer container.Close()

	db := CreateTestClient(container, t)
	defer db.Close()

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

func TestReadStreamReturnsEOFAfterCompletion(t *testing.T) {
	container := GetEmptyDatabase()
	defer container.Close()
	db := CreateTestClient(container, t)
	defer db.Close()

	var waitingForError sync.WaitGroup

	proposedEvents := []esdb.EventData{}

	for i := 1; i <= 10; i++ {
		proposedEvents = append(proposedEvents, createTestEvent())
	}

	opts := esdb.AppendToStreamOptions{
		ExpectedRevision: esdb.NoStream{},
	}
	_, err := db.AppendToStream(context.Background(), "testing-closing", opts, proposedEvents...)
	require.NoError(t, err)

	stream, err := db.ReadStream(context.Background(), "testing-closing", esdb.ReadStreamOptions{}, 1_024)

	require.NoError(t, err)
	_, err = collectStreamEvents(stream)
	require.NoError(t, err)

	go func() {
		_, err := stream.Recv()
		require.Error(t, err)
		require.True(t, err == io.EOF)
		waitingForError.Done()
	}()

	require.NoError(t, err)
	waitingForError.Add(1)
	timedOut := waitWithTimeout(&waitingForError, time.Duration(5)*time.Second)
	require.False(t, timedOut, "Timed out waiting for read stream to return io.EOF on completion")
}

func TestReadStreamNotFound(t *testing.T) {
	container := GetEmptyDatabase()
	defer container.Close()

	db := CreateTestClient(container, t)
	defer db.Close()

	_, err := db.ReadStream(context.Background(), "foobar", esdb.ReadStreamOptions{}, 1)

	require.True(t, errors.Is(err, esdb.ErrStreamNotFound))
}
