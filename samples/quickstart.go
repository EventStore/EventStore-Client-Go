package samples

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"io"

	"github.com/EventStore/EventStore-Client-Go/v4/kurrent"
)

func Run() {
	// region createClient
	settings, err := kurrent.ParseConnectionString("{connectionString}")

	if err != nil {
		panic(err)
	}

	db, err := kurrent.NewClient(settings)

	// endregion createClient
	if err != nil {
		panic(err)
	}

	// region createEvent
	testEvent := TestEvent{
		Id:            uuid.NewString(),
		ImportantData: "I wrote my first event!",
	}

	data, err := json.Marshal(testEvent)

	if err != nil {
		panic(err)
	}

	eventData := kurrent.EventData{
		ContentType: kurrent.ContentTypeJson,
		EventType:   "TestEvent",
		Data:        data,
	}
	// endregion createEvent

	// region appendEvents
	_, err = db.AppendToStream(context.Background(), "some-stream", kurrent.AppendToStreamOptions{}, eventData)
	// endregion appendEvents

	if err != nil {
		panic(err)
	}

	// region readStream
	stream, err := db.ReadStream(context.Background(), "some-stream", kurrent.ReadStreamOptions{}, 10)

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
