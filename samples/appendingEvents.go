package samples

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"log"

	"github.com/EventStore/EventStore-Client-Go/v4/kurrent"
)

type TestEvent struct {
	Id            string
	ImportantData string
}

func AppendToStream(db *kurrent.Client) {
	// region append-to-stream
	data := TestEvent{
		Id:            "1",
		ImportantData: "some value",
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}

	options := kurrent.AppendToStreamOptions{
		ExpectedRevision: kurrent.NoStream{},
	}

	result, err := db.AppendToStream(context.Background(), "some-stream", options, kurrent.EventData{
		ContentType: kurrent.ContentTypeJson,
		EventType:   "some-event",
		Data:        bytes,
	})
	// endregion append-to-stream

	log.Printf("Result: %v", result)
}

func AppendWithSameId(db *kurrent.Client) {
	// region append-duplicate-event
	data := TestEvent{
		Id:            "1",
		ImportantData: "some value",
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}

	id := uuid.New()
	event := kurrent.EventData{
		ContentType: kurrent.ContentTypeJson,
		EventType:   "some-event",
		EventID:     id,
		Data:        bytes,
	}

	_, err = db.AppendToStream(context.Background(), "some-stream", kurrent.AppendToStreamOptions{}, event)

	if err != nil {
		panic(err)
	}

	// attempt to append the same event again
	_, err = db.AppendToStream(context.Background(), "some-stream", kurrent.AppendToStreamOptions{}, event)

	if err != nil {
		panic(err)
	}

	// endregion append-duplicate-event
}

func AppendWithNoStream(db *kurrent.Client) {
	// region append-with-no-stream
	data := TestEvent{
		Id:            "1",
		ImportantData: "some value",
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}

	options := kurrent.AppendToStreamOptions{
		ExpectedRevision: kurrent.NoStream{},
	}

	_, err = db.AppendToStream(context.Background(), "same-event-stream", options, kurrent.EventData{
		ContentType: kurrent.ContentTypeJson,
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
	_, err = db.AppendToStream(context.Background(), "same-event-stream", options, kurrent.EventData{
		ContentType: kurrent.ContentTypeJson,
		EventType:   "some-event",
		Data:        bytes,
	})
	// endregion append-with-no-stream
}

func AppendWithConcurrencyCheck(db *kurrent.Client) {
	// region append-with-concurrency-check
	ropts := kurrent.ReadStreamOptions{
		Direction: kurrent.Backwards,
		From:      kurrent.End{},
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

	aopts := kurrent.AppendToStreamOptions{
		ExpectedRevision: lastEvent.OriginalStreamRevision(),
	}

	_, err = db.AppendToStream(context.Background(), "concurrency-stream", aopts, kurrent.EventData{
		ContentType: kurrent.ContentTypeJson,
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

	_, err = db.AppendToStream(context.Background(), "concurrency-stream", aopts, kurrent.EventData{
		ContentType: kurrent.ContentTypeJson,
		EventType:   "some-event",
		Data:        bytes,
	})
	// endregion append-with-concurrency-check
}

func AppendToStreamOverridingUserCredentials(db *kurrent.Client) {
	data := TestEvent{
		Id:            "1",
		ImportantData: "some value",
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}

	event := kurrent.EventData{
		ContentType: kurrent.ContentTypeJson,
		EventType:   "some-event",
		Data:        bytes,
	}

	// region overriding-user-credentials
	credentials := &kurrent.Credentials{Login: "admin", Password: "changeit"}

	result, err := db.AppendToStream(context.Background(), "some-stream", kurrent.AppendToStreamOptions{Authenticated: credentials}, event)
	// endregion overriding-user-credentials

	log.Printf("Result: %v", result)
}
