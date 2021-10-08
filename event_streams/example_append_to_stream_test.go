package event_streams_test

import (
	"context"
	"fmt"
	"log"

	"github.com/pivonroll/EventStore-Client-Go/event_streams"

	"github.com/google/uuid"

	"github.com/pivonroll/EventStore-Client-Go/connection"
)

// Example of appending an event to a stream which does not exist with WriteStreamRevisionNoStream.
func ExampleClientImpl_AppendToStream_withNoStreamWhenStreamDoesNotExist() {
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

	writeResult, err := client.AppendToStream(context.Background(),
		streamId,
		event_streams.WriteStreamRevisionNoStream{},
		[]event_streams.ProposedEvent{proposedEvent})
	if err != nil {
		log.Fatalln(err)
	}

	if writeResult.GetCurrentRevision() != 0 {
		log.Fatalln(writeResult.GetCurrentRevision())
	}
}

// Example of appending an event to a stream which does not exist with WriteStreamRevisionAny.
func ExampleClientImpl_AppendToStream_withAnyWhenStreamDoesNotExist() {
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

	writeResult, err := client.AppendToStream(context.Background(),
		streamId,
		event_streams.WriteStreamRevisionAny{},
		[]event_streams.ProposedEvent{proposedEvent})
	if err != nil {
		log.Fatalln(err)
	}

	if writeResult.GetCurrentRevision() != 0 {
		log.Fatalln(writeResult.GetCurrentRevision())
	}
}

// Example of appending an event to an existing stream with exact expected stream revision.
func ExampleClientImpl_AppendToStream_withExactStreamRevisionWhenStreamExist() {
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

	firstEvent := event_streams.ProposedEvent{
		EventId:      uuid.Must(uuid.NewRandom()),
		EventType:    "TestEvent",
		ContentType:  "application/octet-stream",
		UserMetadata: []byte{},
		Data:         []byte("some event data"),
	}

	// Create a stream by appending one event to it
	writeResult, err := client.AppendToStream(context.Background(),
		streamId,
		event_streams.WriteStreamRevisionNoStream{},
		[]event_streams.ProposedEvent{firstEvent})
	if err != nil {
		log.Fatalln(err)
	}

	if writeResult.GetCurrentRevision() != 0 {
		log.Fatalln(writeResult.GetCurrentRevision())
	}

	// Append an event to an existing stream
	secondEvent := event_streams.ProposedEvent{
		EventId:      uuid.Must(uuid.NewRandom()),
		EventType:    "TestEvent",
		ContentType:  "application/octet-stream",
		UserMetadata: []byte{},
		Data:         []byte("some event data"),
	}
	writeResult, err = client.AppendToStream(context.Background(),
		streamId,
		event_streams.WriteStreamRevision{Revision: 0}, // 0 because stream has one event, like an index in a slice
		[]event_streams.ProposedEvent{secondEvent})
	if err != nil {
		log.Fatalln(err)
	}

	if writeResult.GetCurrentRevision() != 1 {
		log.Fatalln(writeResult.GetCurrentRevision())
	}
}

// Example of appending an event to an existing stream with WriteStreamRevisionAny.
func ExampleClientImpl_AppendToStream_withAnyStreamRevisionWhenStreamExist() {
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

	firstEvent := event_streams.ProposedEvent{
		EventId:      uuid.Must(uuid.NewRandom()),
		EventType:    "TestEvent",
		ContentType:  "application/octet-stream",
		UserMetadata: []byte{},
		Data:         []byte("some event data"),
	}

	// Create a stream by appending one event to it
	writeResult, err := client.AppendToStream(context.Background(),
		streamId,
		event_streams.WriteStreamRevisionNoStream{},
		[]event_streams.ProposedEvent{firstEvent})
	if err != nil {
		log.Fatalln(err)
	}

	if writeResult.GetCurrentRevision() != 0 {
		log.Fatalln(writeResult.GetCurrentRevision())
	}

	// Append an event to an existing stream
	secondEvent := event_streams.ProposedEvent{
		EventId:      uuid.Must(uuid.NewRandom()),
		EventType:    "TestEvent",
		ContentType:  "application/octet-stream",
		UserMetadata: []byte{},
		Data:         []byte("some event data"),
	}
	writeResult, err = client.AppendToStream(context.Background(),
		streamId,
		event_streams.WriteStreamRevisionAny{},
		[]event_streams.ProposedEvent{secondEvent})
	if err != nil {
		log.Fatalln(err)
	}

	if writeResult.GetCurrentRevision() != 1 {
		log.Fatalln(writeResult.GetCurrentRevision())
	}
}
