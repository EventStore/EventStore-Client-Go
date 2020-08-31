package client_test

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"testing"
	"time"

	direction "github.com/EventStore/EventStore-Client-Go/direction"
	position "github.com/EventStore/EventStore-Client-Go/position"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadAllEventsForwardsFromZeroPosition(t *testing.T) {
	eventsContent, err := ioutil.ReadFile("../resources/test/all-e0-e10.json")
	require.NoError(t, err)

	var testEvents []TestEvent
	err = json.Unmarshal(eventsContent, &testEvents)
	require.NoError(t, err)

	container := GetPrePopulatedDatabase()
	defer container.Close()

	client := CreateTestClient(container, t)
	defer client.Close()

	context, cancel := context.WithTimeout(context.Background(), time.Duration(10)*time.Second)
	defer cancel()

	numberOfEventsToRead := 10
	numberOfEvents := uint64(numberOfEventsToRead)

	events, err := client.ReadAllEvents(context, direction.Forwards, position.StartPosition, numberOfEvents, true)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}

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

func TestReadAllEventsForwardsFromNonZeroPosition(t *testing.T) {
	eventsContent, err := ioutil.ReadFile("../resources/test/all-c1788-p1788.json")
	require.NoError(t, err)

	var testEvents []TestEvent
	err = json.Unmarshal(eventsContent, &testEvents)
	require.NoError(t, err)

	container := GetPrePopulatedDatabase()
	defer container.Close()

	client := CreateTestClient(container, t)
	defer client.Close()

	context, cancel := context.WithTimeout(context.Background(), time.Duration(10)*time.Second)
	defer cancel()

	numberOfEventsToRead := 10
	numberOfEvents := uint64(numberOfEventsToRead)

	events, err := client.ReadAllEvents(context, direction.Forwards, position.Position{Commit: 1788, Prepare: 1788}, numberOfEvents, true)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}

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

func TestReadAllEventsBackwardsFromZeroPosition(t *testing.T) {
	eventsContent, err := ioutil.ReadFile("../resources/test/all-back-e0-e10.json")
	require.NoError(t, err)

	var testEvents []TestEvent
	err = json.Unmarshal(eventsContent, &testEvents)
	require.NoError(t, err)

	container := GetPrePopulatedDatabase()
	defer container.Close()

	client := CreateTestClient(container, t)
	defer client.Close()

	context, cancel := context.WithTimeout(context.Background(), time.Duration(10)*time.Second)
	defer cancel()

	numberOfEventsToRead := 10
	numberOfEvents := uint64(numberOfEventsToRead)

	events, err := client.ReadAllEvents(context, direction.Backwards, position.EndPosition, numberOfEvents, true)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}

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

func TestReadAllEventsBackwardsFromNonZeroPosition(t *testing.T) {
	eventsContent, err := ioutil.ReadFile("../resources/test/all-back-c3386-p3386.json")
	require.NoError(t, err)

	var testEvents []TestEvent
	err = json.Unmarshal(eventsContent, &testEvents)
	require.NoError(t, err)

	container := GetPrePopulatedDatabase()
	defer container.Close()

	client := CreateTestClient(container, t)
	defer client.Close()

	context, cancel := context.WithTimeout(context.Background(), time.Duration(10)*time.Second)
	defer cancel()

	numberOfEventsToRead := 10
	numberOfEvents := uint64(numberOfEventsToRead)

	events, err := client.ReadAllEvents(context, direction.Backwards, position.Position{Commit: 3386, Prepare: 3386}, numberOfEvents, true)

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
