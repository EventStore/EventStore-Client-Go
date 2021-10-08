package event_streams_test

import (
	"context"
	"fmt"
	"log"
	"reflect"

	"github.com/google/uuid"
	"github.com/pivonroll/EventStore-Client-Go/connection"
	"github.com/pivonroll/EventStore-Client-Go/event_streams"
)

// Example of reading events from start of stream $all.
// At the beginning of stream $all we can find system events.
func ExampleClientImpl_ReadEventsFromStreamAll_readEventsFromStart() {
	username := "admin"
	password := "changeit"
	eventStoreEndpoint := "localhost:2113" // assuming that EventStoreDB is running on port 2113
	clientURI := fmt.Sprintf("esdb://%s:%s@%s", username, password, eventStoreEndpoint)
	config, err := connection.ParseConnectionString(clientURI)
	if err != nil {
		log.Fatalln(err)
	}
	grpcClient := connection.NewGrpcClient(*config)
	client := event_streams.ClientFactoryImpl{}.Create(grpcClient)

	// read events from stream $all
	// at the beginning of stream $all are some system events
	// as admin user we can access them too
	readEvents, err := client.ReadEventsFromStreamAll(context.Background(),
		event_streams.ReadDirectionForward,
		event_streams.ReadPositionAllStart{},
		5,
		false)
	if err != nil {
		log.Fatalln(err)
	}

	if len(readEvents) < 5 {
		log.Fatalln("Not enough system events read from stream $all")
	}
}

// Example of reading events backwards from the end of a stream $all.
// We will append some user defined events to demonstrate that user events will
// be read from the end of stream $all in reversed order.
//
// That does not guarantee that system events are not going to be appended after user events
// if some system operation is triggered.
func ExampleClientImpl_ReadEventsFromStreamAll_readEventsBackwardsFromEnd() {
	username := "admin"
	password := "changeit"
	eventStoreEndpoint := "localhost:2113" // assuming that EventStoreDB is running on port 2113
	clientURI := fmt.Sprintf("esdb://%s:%s@%s", username, password, eventStoreEndpoint)
	config, err := connection.ParseConnectionString(clientURI)
	if err != nil {
		log.Fatalln(err)
	}
	grpcClient := connection.NewGrpcClient(*config)
	client := event_streams.ClientFactoryImpl{}.Create(grpcClient)

	streamId := "some_stream"

	// create 10 events to write (EventId must be unique)
	eventsToWrite := make(event_streams.ProposedEventList, 10)
	for i := uint32(0); i < 10; i++ {
		eventsToWrite[i] = event_streams.ProposedEvent{
			EventId:      uuid.Must(uuid.NewRandom()),
			EventType:    "TestEvent",
			ContentType:  "application/octet-stream",
			UserMetadata: []byte{},
			Data:         []byte("some event data"),
		}
	}

	// create a stream with 10 events
	_, err = client.AppendToStream(context.Background(),
		streamId,
		event_streams.WriteStreamRevisionNoStream{},
		eventsToWrite)
	if err != nil {
		log.Fatalln(err)
	}

	count := uint64(len(eventsToWrite))

	readEvents, err := client.ReadEventsFromStreamAll(context.Background(),
		event_streams.ReadDirectionForward,
		event_streams.ReadPositionAllStart{},
		count,
		false)
	if err != nil {
		log.Fatalln(err)
	}

	// Number of events read must equal to count
	if uint64(len(readEvents)) != count {
		log.Fatalln(`Number of events read from stream $all must 
					be greater than number of user defined events`)
	}

	// since events are read backwards from the end they are received in reversed order
	if !reflect.DeepEqual(eventsToWrite, readEvents.Reverse().ToProposedEvents()) {
		log.Fatalln("Events read from the end must match user defined events")
	}
}
