package event_streams_test

import (
	"context"
	"fmt"
	"log"

	"github.com/google/uuid"
	"github.com/pivonroll/EventStore-Client-Go/connection"
	"github.com/pivonroll/EventStore-Client-Go/event_streams"
)

// Example of sot-deleting a stream which exists.
func ExampleClientImpl_DeleteStream_streamExists() {
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
	proposedEvent := event_streams.ProposedEvent{
		EventId:      uuid.Must(uuid.NewRandom()),
		EventType:    "TestEvent",
		ContentType:  "application/octet-stream",
		UserMetadata: []byte{},
		Data:         []byte("some event data"),
	}

	// create a stream with one event
	writeResult, err := client.AppendToStream(context.Background(),
		streamId,
		event_streams.WriteStreamRevisionNoStream{},
		[]event_streams.ProposedEvent{proposedEvent})
	if err != nil {
		log.Fatalln(err)
	}

	// soft-delete a stream
	deleteResult, err := client.DeleteStream(context.Background(),
		streamId,
		event_streams.WriteStreamRevision{Revision: writeResult.GetCurrentRevision()})

	deletePosition, isPosition := deleteResult.GetPosition()

	if !isPosition {
		log.Fatalln("Must be a position")
	}

	writePosition, _ := writeResult.GetPosition()

	// position returned by soft-delete must be greater than the one of the last event
	if !deletePosition.GreaterThan(writePosition) {
		log.Fatalln("Delete position must be greater than last event's write position")
	}
}

// Example of soft-deleting a stream which does not exist.
func ExampleClientImpl_DeleteStream_streamDoesNotExist() {
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

	// delete a non-existing stream
	deleteResult, err := client.DeleteStream(context.Background(),
		streamId,
		event_streams.WriteStreamRevisionNoStream{})
	deletePosition, isPosition := deleteResult.GetPosition()

	// result of a soft-delete must be a position
	if !isPosition {
		log.Fatalln("Must be a position")
	}

	// position returned by soft-delete must not be zero
	if deletePosition.CommitPosition == 0 || deletePosition.PreparePosition == 0 {
		log.Fatalln("Commit and Prepare position must not be zero")
	}
}
