package samples

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/gofrs/uuid"

	"github.com/EventStore/EventStore-Client-Go/v2/esdb"
)

func Run() {
	// region createClient
	settings, err := esdb.ParseConnectionString("{connectionString}")

	if err != nil {
		panic(err)
	}

	db, err := esdb.NewClient(settings)

	// endregion createClient
	if err != nil {
		panic(err)
	}

	// region createEvent
	testEvent := TestEvent{
		Id:            uuid.Must(uuid.NewV4()).String(),
		ImportantData: "I wrote my first event!",
	}

	data, err := json.Marshal(testEvent)

	if err != nil {
		panic(err)
	}

	eventData := esdb.EventData{
		ContentType: esdb.ContentTypeJson,
		EventType:   "TestEvent",
		Data:        data,
	}
	// endregion createEvent

	// region appendEvents
	_, err = db.AppendToStream(context.Background(), "some-stream", esdb.AppendToStreamOptions{}, eventData)
	// endregion appendEvents

	if err != nil {
		panic(err)
	}

	// region readStream
	stream, err := db.ReadStream(context.Background(), "some-stream", esdb.ReadStreamOptions{}, 10)

	if err != nil {
		panic(err)
	}

	defer stream.Close()

	for {
		event, err := stream.Recv()

		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			panic(err)
		}

		// Doing something productive with the event
		fmt.Println(event)
	}
	// endregion readStream
}
