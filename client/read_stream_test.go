package client_test

import (
	"context"
	"testing"
	"time"

	direction "github.com/EventStore/EventStore-Client-Go/direction"
	position "github.com/EventStore/EventStore-Client-Go/position"
	"github.com/EventStore/EventStore-Client-Go/streamrevision"
	uuid "github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
)

func TestReadStreamEventsForwardsFromZeroPosition(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()

	client := CreateTestClient(container, t)
	defer client.Close()

	context, cancel := context.WithTimeout(context.Background(), time.Duration(10)*time.Second)
	defer cancel()

	numberOfEvents := uint64(10)

	streamId := "dataset20M-1800"

	events, err := client.ReadStreamEvents(context, direction.Forwards, streamId, streamrevision.StreamRevisionStart, numberOfEvents, true)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}

	assert.Equal(t, numberOfEvents, uint64(len(events)), "Expected the correct number of messages to be returned")

	expectedEventID, _ := uuid.FromString("f78f41f1-a671-4096-adee-e0c8670fd1ef")
	assert.Equal(t, expectedEventID, events[0].EventID)
	assert.Equal(t, "eventType-1800", events[0].EventType)
	assert.Equal(t, "dataset20M-1800", events[0].StreamID)
	assert.Equal(t, uint64(0), events[0].EventNumber)
	expectedCreated, _ := time.Parse(time.RFC3339, "2020-01-12T18:16:26.245917Z")
	assert.Equal(t, expectedCreated, events[0].CreatedDate)
	assert.Equal(t, position.EmptyPosition, events[0].Position)
	assert.Equal(t, "application/json", events[0].ContentType)

	expectedEventID, _ = uuid.FromString("4ba2bea8-af22-4530-83ea-00556b0e7f39")
	assert.Equal(t, expectedEventID, events[1].EventID)
	assert.Equal(t, "eventType-1801", events[1].EventType)
	assert.Equal(t, "dataset20M-1800", events[1].StreamID)
	assert.Equal(t, uint64(1), events[1].EventNumber)
	expectedCreated, _ = time.Parse(time.RFC3339, "2020-01-12T18:16:26.245936Z")
	assert.Equal(t, expectedCreated, events[1].CreatedDate)
	assert.Equal(t, position.EmptyPosition, events[1].Position)
	assert.Equal(t, "application/json", events[1].ContentType)

	expectedEventID, _ = uuid.FromString("23b32f81-8d9f-4d68-b2dd-4cf8ac6a72a9")
	assert.Equal(t, expectedEventID, events[2].EventID)
	assert.Equal(t, "eventType-1802", events[2].EventType)
	assert.Equal(t, "dataset20M-1800", events[2].StreamID)
	assert.Equal(t, uint64(2), events[2].EventNumber)
	expectedCreated, _ = time.Parse(time.RFC3339, "2020-01-12T18:16:26.24594Z")
	assert.Equal(t, expectedCreated, events[2].CreatedDate)
	assert.Equal(t, position.EmptyPosition, events[2].Position)
	assert.Equal(t, "application/json", events[2].ContentType)

	expectedEventID, _ = uuid.FromString("988595fa-d6ad-4261-9613-168fc8202acb")
	assert.Equal(t, expectedEventID, events[3].EventID)
	assert.Equal(t, "eventType-1803", events[3].EventType)
	assert.Equal(t, "dataset20M-1800", events[3].StreamID)
	assert.Equal(t, uint64(3), events[3].EventNumber)
	expectedCreated, _ = time.Parse(time.RFC3339, "2020-01-12T18:16:26.245943Z")
	assert.Equal(t, expectedCreated, events[3].CreatedDate)
	assert.Equal(t, position.EmptyPosition, events[3].Position)
	assert.Equal(t, "application/json", events[3].ContentType)

	expectedEventID, _ = uuid.FromString("13745afd-841d-4b2f-a0d6-0f7d61a79298")
	assert.Equal(t, expectedEventID, events[4].EventID)
	assert.Equal(t, "eventType-1804", events[4].EventType)
	assert.Equal(t, "dataset20M-1800", events[4].StreamID)
	assert.Equal(t, uint64(4), events[4].EventNumber)
	expectedCreated, _ = time.Parse(time.RFC3339, "2020-01-12T18:16:26.245955Z")
	assert.Equal(t, expectedCreated, events[4].CreatedDate)
	assert.Equal(t, position.EmptyPosition, events[4].Position)
	assert.Equal(t, "application/json", events[4].ContentType)

	expectedEventID, _ = uuid.FromString("0922cb9c-6560-4958-83b1-1de5fd9a0e7e")
	assert.Equal(t, expectedEventID, events[5].EventID)
	assert.Equal(t, "eventType-1805", events[5].EventType)
	assert.Equal(t, "dataset20M-1800", events[5].StreamID)
	assert.Equal(t, uint64(5), events[5].EventNumber)
	expectedCreated, _ = time.Parse(time.RFC3339, "2020-01-12T18:16:26.245958Z")
	assert.Equal(t, expectedCreated, events[5].CreatedDate)
	assert.Equal(t, position.EmptyPosition, events[5].Position)
	assert.Equal(t, "application/json", events[5].ContentType)

	expectedEventID, _ = uuid.FromString("6a53cbe7-7ac4-447f-94cc-edd70943f955")
	assert.Equal(t, expectedEventID, events[6].EventID)
	assert.Equal(t, "eventType-1806", events[6].EventType)
	assert.Equal(t, "dataset20M-1800", events[6].StreamID)
	assert.Equal(t, uint64(6), events[6].EventNumber)
	expectedCreated, _ = time.Parse(time.RFC3339, "2020-01-12T18:16:26.245962Z")
	assert.Equal(t, expectedCreated, events[6].CreatedDate)
	assert.Equal(t, position.EmptyPosition, events[6].Position)
	assert.Equal(t, "application/json", events[6].ContentType)

	expectedEventID, _ = uuid.FromString("0fadd114-46ae-4825-a5cd-257b69de4af8")
	assert.Equal(t, expectedEventID, events[7].EventID)
	assert.Equal(t, "eventType-1807", events[7].EventType)
	assert.Equal(t, "dataset20M-1800", events[7].StreamID)
	assert.Equal(t, uint64(7), events[7].EventNumber)
	expectedCreated, _ = time.Parse(time.RFC3339, "2020-01-12T18:16:26.245964Z")
	assert.Equal(t, expectedCreated, events[7].CreatedDate)
	assert.Equal(t, position.EmptyPosition, events[7].Position)
	assert.Equal(t, "application/json", events[7].ContentType)

	expectedEventID, _ = uuid.FromString("df1aae39-a18b-4372-bc3b-1c0cc81b38c1")
	assert.Equal(t, expectedEventID, events[8].EventID)
	assert.Equal(t, "eventType-1808", events[8].EventType)
	assert.Equal(t, "dataset20M-1800", events[8].StreamID)
	assert.Equal(t, uint64(8), events[8].EventNumber)
	expectedCreated, _ = time.Parse(time.RFC3339, "2020-01-12T18:16:26.245968Z")
	assert.Equal(t, expectedCreated, events[8].CreatedDate)
	assert.Equal(t, position.EmptyPosition, events[8].Position)
	assert.Equal(t, "application/json", events[8].ContentType)

	expectedEventID, _ = uuid.FromString("a6f738f5-8ce5-4580-9aa6-cebd86643d12")
	assert.Equal(t, expectedEventID, events[9].EventID)
	assert.Equal(t, "eventType-1809", events[9].EventType)
	assert.Equal(t, "dataset20M-1800", events[9].StreamID)
	assert.Equal(t, uint64(9), events[9].EventNumber)
	expectedCreated, _ = time.Parse(time.RFC3339, "2020-01-12T18:16:26.24597Z")
	assert.Equal(t, expectedCreated, events[9].CreatedDate)
	assert.Equal(t, position.EmptyPosition, events[9].Position)
	assert.Equal(t, "application/json", events[9].ContentType)
}

func TestReadStreamEventsBackwardsFromEndPosition(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()

	client := CreateTestClient(container, t)
	defer client.Close()

	context, cancel := context.WithTimeout(context.Background(), time.Duration(10)*time.Second)
	defer cancel()

	numberOfEvents := uint64(10)

	streamId := "dataset20M-1800"

	events, err := client.ReadStreamEvents(context, direction.Backwards, streamId, streamrevision.StreamRevisionEnd, numberOfEvents, true)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}

	assert.Equal(t, numberOfEvents, uint64(len(events)), "Expected the correct number of messages to be returned")

	expectedEventID, _ := uuid.FromString("16301e2a-595d-4875-a102-fc19b47d93b3")
	assert.Equal(t, expectedEventID, events[0].EventID)
	assert.Equal(t, "eventType-1999", events[0].EventType)
	assert.Equal(t, "dataset20M-1800", events[0].StreamID)
	assert.Equal(t, uint64(1999), events[0].EventNumber)
	expectedCreated, _ := time.Parse(time.RFC3339, "2020-01-12T18:16:26.52177Z")
	assert.Equal(t, expectedCreated, events[0].CreatedDate)
	assert.Equal(t, position.EmptyPosition, events[0].Position)
	assert.Equal(t, "application/json", events[0].ContentType)

	expectedEventID, _ = uuid.FromString("a2109b1d-4278-4337-98cb-7eee706f2e00")
	assert.Equal(t, expectedEventID, events[1].EventID)
	assert.Equal(t, "eventType-1998", events[1].EventType)
	assert.Equal(t, "dataset20M-1800", events[1].StreamID)
	assert.Equal(t, uint64(1998), events[1].EventNumber)
	expectedCreated, _ = time.Parse(time.RFC3339, "2020-01-12T18:16:26.521767Z")
	assert.Equal(t, expectedCreated, events[1].CreatedDate)
	assert.Equal(t, position.EmptyPosition, events[1].Position)
	assert.Equal(t, "application/json", events[1].ContentType)

	expectedEventID, _ = uuid.FromString("4612f010-db0f-46e4-a8ec-24bc5e81993a")
	assert.Equal(t, expectedEventID, events[2].EventID)
	assert.Equal(t, "eventType-1997", events[2].EventType)
	assert.Equal(t, "dataset20M-1800", events[2].StreamID)
	assert.Equal(t, uint64(1997), events[2].EventNumber)
	expectedCreated, _ = time.Parse(time.RFC3339, "2020-01-12T18:16:26.521765Z")
	assert.Equal(t, expectedCreated, events[2].CreatedDate)
	assert.Equal(t, position.EmptyPosition, events[2].Position)
	assert.Equal(t, "application/json", events[2].ContentType)

	expectedEventID, _ = uuid.FromString("d8e5e5c7-d3dd-4f9b-9940-066efa5f86e4")
	assert.Equal(t, expectedEventID, events[3].EventID)
	assert.Equal(t, "eventType-1996", events[3].EventType)
	assert.Equal(t, "dataset20M-1800", events[3].StreamID)
	assert.Equal(t, uint64(1996), events[3].EventNumber)
	expectedCreated, _ = time.Parse(time.RFC3339, "2020-01-12T18:16:26.521762Z")
	assert.Equal(t, expectedCreated, events[3].CreatedDate)
	assert.Equal(t, position.EmptyPosition, events[3].Position)
	assert.Equal(t, "application/json", events[3].ContentType)

	expectedEventID, _ = uuid.FromString("c6f0b8e4-19db-4272-a3d7-3d49240c6cf1")
	assert.Equal(t, expectedEventID, events[4].EventID)
	assert.Equal(t, "eventType-1995", events[4].EventType)
	assert.Equal(t, "dataset20M-1800", events[4].StreamID)
	assert.Equal(t, uint64(1995), events[4].EventNumber)
	expectedCreated, _ = time.Parse(time.RFC3339, "2020-01-12T18:16:26.521759Z")
	assert.Equal(t, expectedCreated, events[4].CreatedDate)
	assert.Equal(t, position.EmptyPosition, events[4].Position)
	assert.Equal(t, "application/json", events[4].ContentType)

	expectedEventID, _ = uuid.FromString("67b3dcbd-330b-4f65-ad8f-3e873ae9a7e4")
	assert.Equal(t, expectedEventID, events[5].EventID)
	assert.Equal(t, "eventType-1994", events[5].EventType)
	assert.Equal(t, "dataset20M-1800", events[5].StreamID)
	assert.Equal(t, uint64(1994), events[5].EventNumber)
	expectedCreated, _ = time.Parse(time.RFC3339, "2020-01-12T18:16:26.521756Z")
	assert.Equal(t, expectedCreated, events[5].CreatedDate)
	assert.Equal(t, position.EmptyPosition, events[5].Position)
	assert.Equal(t, "application/json", events[5].ContentType)

	expectedEventID, _ = uuid.FromString("6f760d99-8730-482f-a662-f9bed739d91f")
	assert.Equal(t, expectedEventID, events[6].EventID)
	assert.Equal(t, "eventType-1993", events[6].EventType)
	assert.Equal(t, "dataset20M-1800", events[6].StreamID)
	assert.Equal(t, uint64(1993), events[6].EventNumber)
	expectedCreated, _ = time.Parse(time.RFC3339, "2020-01-12T18:16:26.521753Z")
	assert.Equal(t, expectedCreated, events[6].CreatedDate)
	assert.Equal(t, position.EmptyPosition, events[6].Position)
	assert.Equal(t, "application/json", events[6].ContentType)

	expectedEventID, _ = uuid.FromString("5450f5f6-370d-420d-93fd-c5ac51e2b77e")
	assert.Equal(t, expectedEventID, events[7].EventID)
	assert.Equal(t, "eventType-1992", events[7].EventType)
	assert.Equal(t, "dataset20M-1800", events[7].StreamID)
	assert.Equal(t, uint64(1992), events[7].EventNumber)
	expectedCreated, _ = time.Parse(time.RFC3339, "2020-01-12T18:16:26.52175Z")
	assert.Equal(t, expectedCreated, events[7].CreatedDate)
	assert.Equal(t, position.EmptyPosition, events[7].Position)
	assert.Equal(t, "application/json", events[7].ContentType)

	expectedEventID, _ = uuid.FromString("ff4abbfb-6f8a-4ca4-85b4-999f3ee18239")
	assert.Equal(t, expectedEventID, events[8].EventID)
	assert.Equal(t, "eventType-1991", events[8].EventType)
	assert.Equal(t, "dataset20M-1800", events[8].StreamID)
	assert.Equal(t, uint64(1991), events[8].EventNumber)
	expectedCreated, _ = time.Parse(time.RFC3339, "2020-01-12T18:16:26.521748Z")
	assert.Equal(t, expectedCreated, events[8].CreatedDate)
	assert.Equal(t, position.EmptyPosition, events[8].Position)
	assert.Equal(t, "application/json", events[8].ContentType)

	expectedEventID, _ = uuid.FromString("4c45f3af-605f-4973-9198-07f0579a279a")
	assert.Equal(t, expectedEventID, events[9].EventID)
	assert.Equal(t, "eventType-1990", events[9].EventType)
	assert.Equal(t, "dataset20M-1800", events[9].StreamID)
	assert.Equal(t, uint64(1990), events[9].EventNumber)
	expectedCreated, _ = time.Parse(time.RFC3339, "2020-01-12T18:16:26.521745Z")
	assert.Equal(t, expectedCreated, events[9].CreatedDate)
	assert.Equal(t, position.EmptyPosition, events[9].Position)
	assert.Equal(t, "application/json", events[9].ContentType)
}
