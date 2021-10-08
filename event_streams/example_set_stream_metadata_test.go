package event_streams_test

import (
	"context"
	"fmt"
	"log"
	"reflect"

	"github.com/google/uuid"
	"github.com/pivonroll/EventStore-Client-Go/connection"
	"github.com/pivonroll/EventStore-Client-Go/event_streams"
	"github.com/pivonroll/EventStore-Client-Go/ptr"
)

// Example of setting metadata for a stream which does not exist.
func ExampleClientImpl_SetStreamMetadata_onNonExistingStream() {
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

	expectedStreamMetadata := event_streams.StreamMetadata{
		MaxCount:              ptr.Int(17),
		TruncateBefore:        ptr.UInt64(10),
		CacheControlInSeconds: ptr.UInt64(17),
		MaxAgeInSeconds:       ptr.UInt64(15),
	}

	// write metadata for a stream
	_, err = client.SetStreamMetadata(context.Background(),
		streamId,
		event_streams.WriteStreamRevisionNoStream{},
		expectedStreamMetadata,
	)
	if err != nil {
		log.Fatalln(err)
	}

	// read metadata for a stream
	metaData, err := client.GetStreamMetadata(context.Background(), streamId)
	if err != nil {
		log.Fatalln(err)
	}

	if metaData.IsEmpty() {
		log.Fatalln("Stream must have metadata")
	}

	if metaData.GetMetaStreamRevision() != 0 {
		log.Fatalln("Metadata must be at index 0 in stream's metadata stream")
	}

	if !reflect.DeepEqual(expectedStreamMetadata, metaData.GetStreamMetadata()) {
		log.Fatalln("Metadata received must be the same as the metadata written")
	}
}

// Example of setting metadata for an existing stream.
func ExampleClientImpl_SetStreamMetadata_whenStreamExists() {
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
	_, err = client.AppendToStream(context.Background(),
		streamId,
		event_streams.WriteStreamRevisionNoStream{},
		[]event_streams.ProposedEvent{proposedEvent})
	if err != nil {
		log.Fatalln(err)
	}

	expectedStreamMetadata := event_streams.StreamMetadata{
		MaxCount:              ptr.Int(17),
		TruncateBefore:        ptr.UInt64(10),
		CacheControlInSeconds: ptr.UInt64(17),
		MaxAgeInSeconds:       ptr.UInt64(15),
	}

	// write metadata for a stream
	_, err = client.SetStreamMetadata(context.Background(),
		streamId,
		event_streams.WriteStreamRevisionNoStream{},
		expectedStreamMetadata,
	)
	if err != nil {
		log.Fatalln(err)
	}

	// read stream's metadata
	metaData, err := client.GetStreamMetadata(context.Background(), streamId)
	if err != nil {
		log.Fatalln(err)
	}

	if metaData.IsEmpty() {
		log.Fatalln("Stream must have metadata")
	}

	if metaData.GetMetaStreamRevision() != 0 {
		log.Fatalln("Metadata must be at index 0 in stream's metadata stream")
	}

	if !reflect.DeepEqual(expectedStreamMetadata, metaData.GetStreamMetadata()) {
		log.Fatalln("Metadata received must be the same as the metadata written")
	}
}
