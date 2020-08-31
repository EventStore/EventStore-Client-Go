package client_test

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"testing"
	"time"

	direction "github.com/EventStore/EventStore-Client-Go/direction"
	"github.com/EventStore/EventStore-Client-Go/streamrevision"
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

	client := CreateTestClient(container, t)
	defer client.Close()

	context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
	defer cancel()

	numberOfEventsToRead := 10
	numberOfEvents := uint64(numberOfEventsToRead)

	streamId := "dataset20M-1800"

	events, err := client.ReadStreamEvents(context, direction.Forwards, streamId, streamrevision.StreamRevisionStart, numberOfEvents, true)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}

	assert.Equal(t, numberOfEvents, uint64(len(events)), "Expected the correct number of messages to be returned")

	for i := 0; i < numberOfEventsToRead; i++ {
		assert.Equal(t, testEvents[i].Event.EventID, events[i].EventID)
		assert.Equal(t, testEvents[i].Event.EventType, events[i].EventType)
		assert.Equal(t, testEvents[i].Event.StreamID, events[i].StreamID)
		assert.Equal(t, testEvents[i].Event.StreamRevision.Value, events[i].EventNumber)
		assert.Equal(t, testEvents[i].Event.Created.Nanos, events[i].CreatedDate.Nanosecond())
		assert.Equal(t, testEvents[i].Event.Created.Seconds, events[i].CreatedDate.Unix())
		assert.Equal(t, testEvents[i].Event.Position.Commit, events[i].Position.Commit)
		assert.Equal(t, testEvents[i].Event.Position.Prepare, events[i].Position.Prepare)
		assert.Equal(t, testEvents[i].Event.ContentType, events[i].ContentType)
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

	client := CreateTestClient(container, t)
	defer client.Close()

	context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
	defer cancel()

	numberOfEventsToRead := 10
	numberOfEvents := uint64(numberOfEventsToRead)

	streamId := "dataset20M-1800"

	events, err := client.ReadStreamEvents(context, direction.Backwards, streamId, streamrevision.StreamRevisionEnd, numberOfEvents, true)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}

	assert.Equal(t, numberOfEvents, uint64(len(events)), "Expected the correct number of messages to be returned")

	for i := 0; i < numberOfEventsToRead; i++ {
		assert.Equal(t, testEvents[i].Event.EventID, events[i].EventID)
		assert.Equal(t, testEvents[i].Event.EventType, events[i].EventType)
		assert.Equal(t, testEvents[i].Event.StreamID, events[i].StreamID)
		assert.Equal(t, testEvents[i].Event.StreamRevision.Value, events[i].EventNumber)
		assert.Equal(t, testEvents[i].Event.Created.Nanos, events[i].CreatedDate.Nanosecond())
		assert.Equal(t, testEvents[i].Event.Created.Seconds, events[i].CreatedDate.Unix())
		assert.Equal(t, testEvents[i].Event.Position.Commit, events[i].Position.Commit)
		assert.Equal(t, testEvents[i].Event.Position.Prepare, events[i].Position.Prepare)
		assert.Equal(t, testEvents[i].Event.ContentType, events[i].ContentType)
	}
}
