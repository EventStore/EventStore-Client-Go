package client_test

import (
	"context"
	"testing"
	"time"

	direction "github.com/eventstore/EventStore-Client-Go/direction"
	position "github.com/eventstore/EventStore-Client-Go/position"
	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
)

func TestReads(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()

	t.Run("TestReadAllEventsForwardsFromZeroPosition", func(t *testing.T) {
		client := CreateTestClient(container, t)
		defer client.Close()

		context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
		defer cancel()

		numberOfEvents := uint64(10)

		events, err := client.ReadAllEvents(context, direction.Forwards, position.StartPosition, numberOfEvents, true)

		if err != nil {
			t.Fatalf("Unexpected failure %+v", err)
		}

		assert.Equal(t, numberOfEvents, uint64(len(events)), "Expected the correct number of messages to be returned")
		expectedEventID, _ := uuid.FromString("5ed0b2c9-dde9-4312-941e-19c494536f37")
		assert.Equal(t, expectedEventID, events[0].EventID)
		assert.Equal(t, "$metadata", events[0].EventType)
		assert.Equal(t, "$$$stats-0.0.0.0:2113", events[0].StreamID)
		assert.Equal(t, uint64(0), events[0].StreamRevision)
		expectedCreated, _ := time.Parse(time.RFC3339, "2020-01-12T18:14:13.990951Z")
		assert.Equal(t, expectedCreated, events[0].CreatedDate)
		assert.Equal(t, position.Position{Commit: 167, Prepare: 167}, events[0].Position)
		assert.Equal(t, true, events[0].IsJSON)

		expectedEventID, _ = uuid.FromString("4936a85f-e6cb-4a72-9007-ceed0c8a56e7")
		assert.Equal(t, expectedEventID, events[9].EventID)
		assert.Equal(t, "$metadata", events[9].EventType)
		assert.Equal(t, "$$$projections-$0c745883dffc440e89dcf4f511e81101", events[9].StreamID)
		assert.Equal(t, uint64(0), events[9].StreamRevision)
		expectedCreated, _ = time.Parse(time.RFC3339, "2020-01-12T18:14:14.108422Z")
		assert.Equal(t, expectedCreated, events[9].CreatedDate)
		assert.Equal(t, position.Position{Commit: 1788, Prepare: 1788}, events[9].Position)
		assert.Equal(t, true, events[9].IsJSON)
	})

	t.Run("TestReadAllEventsForwardsFromNonZeroPosition", func(t *testing.T) {
		client := CreateTestClient(container, t)
		defer client.Close()

		context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
		defer cancel()

		numberOfEvents := uint64(10)

		events, err := client.ReadAllEvents(context, direction.Forwards, position.Position{Commit: 1788, Prepare: 1788}, numberOfEvents, true)

		if err != nil {
			t.Fatalf("Unexpected failure %+v", err)
		}

		assert.Equal(t, numberOfEvents, uint64(len(events)), "Expected the correct number of messages to be returned")
		expectedEventID, _ := uuid.FromString("4936a85f-e6cb-4a72-9007-ceed0c8a56e7")
		assert.Equal(t, expectedEventID, events[0].EventID)
		assert.Equal(t, "$metadata", events[0].EventType)
		assert.Equal(t, "$$$projections-$0c745883dffc440e89dcf4f511e81101", events[0].StreamID)
		assert.Equal(t, uint64(0), events[0].StreamRevision)
		expectedCreated, _ := time.Parse(time.RFC3339, "2020-01-12T18:14:14.108422Z")
		assert.Equal(t, expectedCreated, events[0].CreatedDate)
		assert.Equal(t, position.Position{Commit: 1788, Prepare: 1788}, events[0].Position)
		assert.Equal(t, true, events[0].IsJSON)

		expectedEventID, _ = uuid.FromString("c4bde754-dc19-4835-9005-1e82002ecc10")
		assert.Equal(t, expectedEventID, events[9].EventID)
		assert.Equal(t, "$ProjectionsInitialized", events[9].EventType)
		assert.Equal(t, "$projections-$all", events[9].StreamID)
		assert.Equal(t, uint64(0), events[9].StreamRevision)
		expectedCreated, _ = time.Parse(time.RFC3339, "2020-01-12T18:14:14.158068Z")
		assert.Equal(t, expectedCreated, events[9].CreatedDate)
		assert.Equal(t, position.Position{Commit: 3256, Prepare: 3256}, events[9].Position)
		assert.Equal(t, false, events[9].IsJSON)
	})

	t.Run("TestReadAllEventsBackwardsFromZeroPosition", func(t *testing.T) {
		client := CreateTestClient(container, t)
		defer client.Close()

		context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
		defer cancel()

		numberOfEvents := uint64(10)

		events, err := client.ReadAllEvents(context, direction.Backwards, position.EndPosition, numberOfEvents, true)

		if err != nil {
			t.Fatalf("Unexpected failure %+v", err)
		}

		assert.Equal(t, numberOfEvents, uint64(len(events)), "Expected the correct number of messages to be returned")
		expectedEventID, _ := uuid.FromString("01b58e8c-260c-4f77-a93d-c92edcc255c1")
		assert.Equal(t, expectedEventID, events[0].EventID)
		assert.Equal(t, "$statsCollected", events[0].EventType)
		assert.Equal(t, "$stats-0.0.0.0:2113", events[0].StreamID)
		assert.Equal(t, uint64(11), events[0].StreamRevision)
		expectedCreated, _ := time.Parse(time.RFC3339, "2020-01-12T18:20:14.583749Z")
		assert.Equal(t, expectedCreated, events[0].CreatedDate)
		assert.Equal(t, position.Position{Commit: 20492574, Prepare: 20492574}, events[0].Position)
		assert.Equal(t, true, events[0].IsJSON)
	})

	t.Run("TestReadAllEventsBackwardsFromNonZeroPosition", func(t *testing.T) {
		client := CreateTestClient(container, t)
		defer client.Close()

		context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
		defer cancel()

		numberOfEvents := uint64(10)

		events, err := client.ReadAllEvents(context, direction.Backwards, position.Position{Commit: 3386, Prepare: 3386}, numberOfEvents, true)

		if err != nil {
			t.Fatalf("Unexpected failure %+v", err)
		}

		assert.Equal(t, numberOfEvents, uint64(len(events)), "Expected the correct number of messages to be returned")

		expectedEventID, _ := uuid.FromString("c4bde754-dc19-4835-9005-1e82002ecc10")
		assert.Equal(t, expectedEventID, events[0].EventID)
		assert.Equal(t, "$ProjectionsInitialized", events[0].EventType)
		assert.Equal(t, "$projections-$all", events[0].StreamID)
		assert.Equal(t, uint64(0), events[0].StreamRevision)
		expectedCreated, _ := time.Parse(time.RFC3339, "2020-01-12T18:14:14.158068Z")
		assert.Equal(t, expectedCreated, events[0].CreatedDate)
		assert.Equal(t, position.Position{Commit: 3256, Prepare: 3256}, events[0].Position)
		assert.Equal(t, false, events[0].IsJSON)

		expectedEventID, _ = uuid.FromString("4936a85f-e6cb-4a72-9007-ceed0c8a56e7")
		assert.Equal(t, expectedEventID, events[9].EventID)
		assert.Equal(t, "$metadata", events[9].EventType)
		assert.Equal(t, "$$$projections-$0c745883dffc440e89dcf4f511e81101", events[9].StreamID)
		assert.Equal(t, uint64(0), events[9].StreamRevision)
		expectedCreated, _ = time.Parse(time.RFC3339, "2020-01-12T18:14:14.108422Z")
		assert.Equal(t, expectedCreated, events[9].CreatedDate)
		assert.Equal(t, position.Position{Commit: 1788, Prepare: 1788}, events[9].Position)
		assert.Equal(t, true, events[9].IsJSON)
	})
}
