package event_streams_test

import (
	"context"
	"fmt"
	"log"

	"github.com/google/uuid"
	"github.com/pivonroll/EventStore-Client-Go/connection"
	"github.com/pivonroll/EventStore-Client-Go/event_streams"
)

// Example of putting a tombstone on an existing stream.
func ExampleClientImpl_TombstoneStream_streamExists() {
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

	streamName := "some_stream"
	proposedEvent := event_streams.ProposedEvent{
		EventId:      uuid.Must(uuid.NewRandom()),
		EventType:    "TestEvent",
		ContentType:  "application/octet-stream",
		UserMetadata: []byte{},
		Data:         []byte("some event data"),
	}

	// create a stream with one event
	writeResult, err := client.AppendToStream(context.Background(),
		streamName,
		event_streams.WriteStreamRevisionNoStream{},
		[]event_streams.ProposedEvent{proposedEvent})
	if err != nil {
		log.Fatalln(err)
	}

	// put tombstone on a stream
	tombstoneResult, err := client.TombstoneStream(context.Background(),
		streamName,
		event_streams.WriteStreamRevision{Revision: writeResult.GetCurrentRevision()})

	tombstonePosition, isPosition := tombstoneResult.GetPosition()

	if !isPosition {
		log.Fatalln("Must be a position")
	}

	writePosition, _ := writeResult.GetPosition()

	// position returned after we put a tombstone on a stream must be greater
	// than the one of the last event
	if !tombstonePosition.GreaterThan(writePosition) {
		log.Fatalln("Delete position must be greater than last event's write position")
	}
}

// Example of trying to put a tombstone on a stream which does not exist.
func ExampleClientImpl_TombstoneStream_streamDoesNotExist() {
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

	streamName := "some_stream"

	// tombstone a non-existing stream
	tombstoneResult, err := client.TombstoneStream(context.Background(),
		streamName,
		event_streams.WriteStreamRevisionNoStream{})
	tombstonePosition, isPosition := tombstoneResult.GetPosition()

	// result of a hard-delete must be a position
	if !isPosition {
		log.Fatalln("Must be a position")
	}

	// position returned by hard-delete must not be zero
	if tombstonePosition.CommitPosition == 0 || tombstonePosition.PreparePosition == 0 {
		log.Fatalln("Commit and Prepare position must not be zero")
	}
}
