package samples

import (
	"context"
	"encoding/json"
	"log"

	"github.com/gofrs/uuid"

	"github.com/EventStore/EventStore-Client-Go/v2/esdb"
)

type TestEvent struct {
	Id            string
	ImportantData string
}

func AppendToStream(db *esdb.Client) {
	// region append-to-stream
	data := TestEvent{
		Id:            "1",
		ImportantData: "some value",
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}

	options := esdb.AppendToStreamOptions{
		ExpectedRevision: esdb.NoStream{},
	}

	result, err := db.AppendToStream(context.Background(), "some-stream", options, esdb.EventData{
		ContentType: esdb.ContentTypeJson,
		EventType:   "some-event",
		Data:        bytes,
	})
	// endregion append-to-stream

	log.Printf("Result: %v", result)
}

func AppendWithSameId(db *esdb.Client) {
	// region append-duplicate-event
	data := TestEvent{
		Id:            "1",
		ImportantData: "some value",
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}

	id := uuid.Must(uuid.NewV4())
	event := esdb.EventData{
		ContentType: esdb.ContentTypeJson,
		EventType:   "some-event",
		EventID:     id,
		Data:        bytes,
	}

	_, err = db.AppendToStream(context.Background(), "some-stream", esdb.AppendToStreamOptions{}, event)

	if err != nil {
		panic(err)
	}

	// attempt to append the same event again
	_, err = db.AppendToStream(context.Background(), "some-stream", esdb.AppendToStreamOptions{}, event)

	if err != nil {
		panic(err)
	}

	// endregion append-duplicate-event
}

func AppendWithNoStream(db *esdb.Client) {
	// region append-with-no-stream
	data := TestEvent{
		Id:            "1",
		ImportantData: "some value",
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}

	options := esdb.AppendToStreamOptions{
		ExpectedRevision: esdb.NoStream{},
	}

	_, err = db.AppendToStream(context.Background(), "same-event-stream", options, esdb.EventData{
		ContentType: esdb.ContentTypeJson,
		EventType:   "some-event",
		Data:        bytes,
	})

	if err != nil {
		panic(err)
	}

	bytes, err = json.Marshal(TestEvent{
		Id:            "2",
		ImportantData: "some other value",
	})
	if err != nil {
		panic(err)
	}

	// attempt to append the same event again
	_, err = db.AppendToStream(context.Background(), "same-event-stream", options, esdb.EventData{
		ContentType: esdb.ContentTypeJson,
		EventType:   "some-event",
		Data:        bytes,
	})
	// endregion append-with-no-stream
}

func AppendWithConcurrencyCheck(db *esdb.Client) {
	// region append-with-concurrency-check
	ropts := esdb.ReadStreamOptions{
		Direction: esdb.Backwards,
		From:      esdb.End{},
	}

	stream, err := db.ReadStream(context.Background(), "concurrency-stream", ropts, 1)

	if err != nil {
		panic(err)
	}

	defer stream.Close()

	lastEvent, err := stream.Recv()

	if err != nil {
		panic(err)
	}

	data := TestEvent{
		Id:            "1",
		ImportantData: "clientOne",
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}

	aopts := esdb.AppendToStreamOptions{
		ExpectedRevision: esdb.Revision(lastEvent.OriginalEvent().EventNumber),
	}

	_, err = db.AppendToStream(context.Background(), "concurrency-stream", aopts, esdb.EventData{
		ContentType: esdb.ContentTypeJson,
		EventType:   "some-event",
		Data:        bytes,
	})

	data = TestEvent{
		Id:            "1",
		ImportantData: "clientTwo",
	}
	bytes, err = json.Marshal(data)
	if err != nil {
		panic(err)
	}

	_, err = db.AppendToStream(context.Background(), "concurrency-stream", aopts, esdb.EventData{
		ContentType: esdb.ContentTypeJson,
		EventType:   "some-event",
		Data:        bytes,
	})
	// endregion append-with-concurrency-check
}

func AppendToStreamOverridingUserCredentials(db *esdb.Client) {
	data := TestEvent{
		Id:            "1",
		ImportantData: "some value",
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}

	event := esdb.EventData{
		ContentType: esdb.ContentTypeJson,
		EventType:   "some-event",
		Data:        bytes,
	}

	// region overriding-user-credentials
	credentials := &esdb.Credentials{Login: "admin", Password: "changeit"}

	result, err := db.AppendToStream(context.Background(), "some-stream", esdb.AppendToStreamOptions{Authenticated: credentials}, event)
	// endregion overriding-user-credentials

	log.Printf("Result: %v", result)
}
