package event_streams_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/pivonroll/EventStore-Client-Go/connection"
	"github.com/pivonroll/EventStore-Client-Go/event_streams"
)

// Example demonstrates how to do a batch append to a stream which does not exist.
// Correlation id for events will auto be generated.
func ExampleClientImpl_BatchAppendToStream_streamDoesNotExist() {
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

	deadline := time.Now().Add(time.Second * 10)
	// batch append to a stream which does not exist
	writeResult, err := client.BatchAppendToStream(context.Background(),
		streamId,
		event_streams.WriteStreamRevisionNoStream{},
		[]event_streams.ProposedEvent{firstEvent},
		1,
		deadline)

	if writeResult.IsCurrentRevisionNoStream() {
		log.Fatalln("IsCurrentRevisionNoStream should return false")
	}

	if writeResult.GetCurrentRevision() != 0 {
		log.Fatalln("Current revision should be 0")
	}
}

// Example demonstrates how to do a batch append to a stream which does exist.
// Correlation id for events will auto be generated.
func ExampleClientImpl_BatchAppendToStream_streamExists() {
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

	// create a stream by appending one event to it
	writeResult, err := client.AppendToStream(context.Background(),
		streamId,
		event_streams.WriteStreamRevisionNoStream{},
		[]event_streams.ProposedEvent{firstEvent})
	if err != nil {
		log.Fatalln(err)
	}

	if writeResult.GetCurrentRevision() != 0 {
		log.Fatalln("Current revision must be 0")
	}

	secondEvent := event_streams.ProposedEvent{
		EventId:      uuid.Must(uuid.NewRandom()),
		EventType:    "TestEvent",
		ContentType:  "application/octet-stream",
		UserMetadata: []byte{},
		Data:         []byte("some event data"),
	}

	deadline := time.Now().Add(time.Second * 10)
	// batch append to a stream which exists with expected revision
	batchWriteResult, err := client.BatchAppendToStream(context.Background(),
		streamId,
		event_streams.WriteStreamRevision{Revision: 0}, // there is already one event in the stream
		[]event_streams.ProposedEvent{secondEvent},
		1,
		deadline)
	if err != nil {
		log.Fatalln(err)
	}

	if batchWriteResult.IsCurrentRevisionNoStream() {
		log.Fatalln("IsCurrentRevisionNoStream should return false")
	}

	if batchWriteResult.GetCurrentRevision() != 0 {
		log.Fatalln("Current revision should be 0")
	}
}

// Example demonstrates how to do a batch append to a stream with correlation id.
func ExampleClientImpl_BatchAppendToStreamWithCorrelationId() {
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

	deadline := time.Now().Add(time.Second * 10)
	correlationId := uuid.New()
	// batch append to a stream with correlation id
	writeResult, err := client.BatchAppendToStreamWithCorrelationId(context.Background(),
		streamId,
		event_streams.WriteStreamRevisionNoStream{},
		correlationId,
		[]event_streams.ProposedEvent{firstEvent},
		1,
		deadline)

	if writeResult.IsCurrentRevisionNoStream() {
		log.Fatalln("IsCurrentRevisionNoStream should return false")
	}

	if writeResult.GetCurrentRevision() != 0 {
		log.Fatalln("Current revision should be 0")
	}
}
