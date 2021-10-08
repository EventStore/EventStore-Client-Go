package event_streams_test

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pivonroll/EventStore-Client-Go/connection"
	"github.com/pivonroll/EventStore-Client-Go/errors"
	"github.com/pivonroll/EventStore-Client-Go/event_streams"
)

// Example show how to subscribe to a stream which does not exist and wait for an event from it.
// We can subscribe to a non-existing stream.
// ReadOne method of StreamReader will block until stream with content is created.
func ExampleClientImpl_SubscribeToStream_streamDoesNotExist() {
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

	wg := sync.WaitGroup{}
	wg.Add(1)

	// subscribe to a stream
	// we can subscribe to a stream which does not exist
	// as soon as stream is created we will start to receive content from it.
	streamReader, err := client.SubscribeToStream(context.Background(),
		streamId,
		event_streams.ReadStreamRevisionStart{},
		false)
	if err != nil {
		log.Fatalln(err)
	}

	proposedEvent := event_streams.ProposedEvent{
		EventId:      uuid.Must(uuid.NewRandom()),
		EventType:    "TestEvent",
		ContentType:  "application/octet-stream",
		UserMetadata: []byte{},
		Data:         []byte("some event data"),
	}

	// we will wait for events in a separate go routine
	go func() {
		defer wg.Done()

		response, err := streamReader.ReadOne() // read blocks until event is written to a stream
		if err != nil {
			log.Fatalln(err)
		}

		event, isEvent := response.GetEvent()
		if !isEvent {
			log.Fatalln("Must have received an event")
		}

		if !reflect.DeepEqual(proposedEvent, event.ToProposedEvent()) {
			log.Fatalln("Must receive an event we have written to a stream")
		}
	}()

	// create a stream with one event written to it
	_, err = client.AppendToStream(context.Background(),
		streamId,
		event_streams.WriteStreamRevisionNoStream{},
		event_streams.ProposedEventList{proposedEvent})

	wg.Wait()
}

// Example shows how to subscribe to an existing stream from start.
func ExampleClientImpl_SubscribeToStream_streamExists() {
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
	_, err = client.AppendToStream(context.Background(),
		streamId,
		event_streams.WriteStreamRevisionNoStream{},
		event_streams.ProposedEventList{proposedEvent})
	if err != nil {
		log.Fatalln(err)
	}

	// subscribe to a stream fro start
	streamReader, err := client.SubscribeToStream(context.Background(),
		streamId,
		event_streams.ReadStreamRevisionStart{},
		false)
	if err != nil {
		log.Fatalln(err)
	}

	wg := sync.WaitGroup{}
	wg.Add(1)
	// we will wait for an event in a separate go routine
	go func() {
		defer wg.Done()

		response, err := streamReader.ReadOne() // read an event written to a stream
		if err != nil {
			log.Fatalln(err)
		}

		event, isEvent := response.GetEvent()
		if !isEvent {
			log.Fatalln("Must have received an event")
		}

		if !reflect.DeepEqual(proposedEvent, event.ToProposedEvent()) {
			log.Fatalln("Must receive an event we have written to a stream")
		}
	}()

	wg.Wait()
}

// Example demonstrates that subscription will receive StreamDeleted error once stream has been deleted.
func ExampleClientImpl_SubscribeToStream_catchesDeletion() {
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

	wg := sync.WaitGroup{}
	wg.Add(1)

	ctx := context.Background()

	// create a subscription to a stream, from start of the stream
	streamReader, err := client.SubscribeToStream(ctx,
		streamId,
		event_streams.ReadStreamRevisionStart{},
		false)
	if err != nil {
		log.Fatalln(err)
	}

	go func() {
		defer wg.Done()
		_, err := streamReader.ReadOne() // reads content of a stream until StreamDeletedErr is received

		if err.Code() != errors.StreamDeletedErr {
			log.Fatalln("Unexpected error received")
		}
	}()

	_, err = client.TombstoneStream(context.Background(),
		streamId,
		event_streams.WriteStreamRevisionNoStream{})
	if err != nil {
		log.Fatalln(err)
	}

	// wait for reader to receive StreamDeleted
	wg.Wait()
}

// Example shows that subscription from start of the stream will receive all old events written to it,
// as well as new events written to a stream after a subscription was created.
func ExampleClientImpl_SubscribeToStream_readOldAndNewContentFromStream() {
	username := "admin"
	password := "changeit"
	eventStoreEndpoint := "localhost:2113" // assuming that EventStoreDB is running on port 2113
	clientURI := fmt.Sprintf("esdb://%s:%s@%s", username, password, eventStoreEndpoint)
	config, stdErr := connection.ParseConnectionString(clientURI)
	if stdErr != nil {
		log.Fatalln(stdErr)
	}
	grpcClient := connection.NewGrpcClient(*config)
	client := event_streams.ClientFactoryImpl{}.Create(grpcClient)

	streamId := "some_stream"

	readerWait := sync.WaitGroup{}
	readerWait.Add(1)

	cancelWait := sync.WaitGroup{}
	cancelWait.Add(1)

	ctx := context.Background()
	ctx, cancelFunc := context.WithTimeout(ctx, 20*time.Second)

	createEvents := func(eventCount uint32) event_streams.ProposedEventList {
		result := make(event_streams.ProposedEventList, eventCount)

		for i := uint32(0); i < eventCount; i++ {
			result = append(result, event_streams.ProposedEvent{
				EventId:      uuid.Must(uuid.NewRandom()),
				EventType:    "TestEvent",
				ContentType:  "application/octet-stream",
				UserMetadata: []byte{},
				Data:         []byte("some event data"),
			})
		}
		return result
	}
	beforeEvents := createEvents(3)
	afterEvents := createEvents(2)

	totalEvents := append(beforeEvents, afterEvents...)

	// create a stream with 3 events in it
	_, err := client.AppendToStream(context.Background(),
		streamId,
		event_streams.WriteStreamRevisionNoStream{},
		beforeEvents)
	if err != nil {
		log.Fatalln(err)
	}

	// create a stream subscription from start of the stream
	streamReader, err := client.SubscribeToStream(ctx,
		streamId,
		event_streams.ReadStreamRevisionStart{},
		false)
	if err != nil {
		log.Fatalln(err)
	}

	go func() {
		defer readerWait.Done()

		var result event_streams.ProposedEventList

		for {
			response, err := streamReader.ReadOne() // read event one by-one
			if err != nil {
				if err.Code() == errors.CanceledErr { // we have received cancellation of a subscription
					break
				}
				cancelWait.Done() // must never be reached, some other error occur
				log.Fatalln("Unexpected error received")
			}

			event, isEvent := response.GetEvent()

			if !isEvent {
				log.Fatalln("Must have read an event")
			}
			result = append(result, event.ToProposedEvent())
			if len(result) == len(totalEvents) {
				cancelWait.Done() // we have read all events, signal to main thread that it can cancel subscription
			}
		}

		if !reflect.DeepEqual(totalEvents, result) {
			log.Fatalln("Not all events have been read from the stream")
		}
	}()

	_, err = client.AppendToStream(context.Background(),
		streamId,
		event_streams.WriteStreamRevisionAny{},
		afterEvents)

	cancelWait.Wait() // wait until subscription receives all events
	cancelFunc()      // cancel subscription

	readerWait.Wait() // wait for subscription go routine to exit
}
