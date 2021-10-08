package event_streams_test

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"sync"

	"github.com/google/uuid"
	"github.com/pivonroll/EventStore-Client-Go/connection"
	"github.com/pivonroll/EventStore-Client-Go/event_streams"
	"github.com/pivonroll/EventStore-Client-Go/systemmetadata"
)

// Example demonstrates how to subscribe to stream $all with filter.
//
// We create three streams and write events to them.
// Subscription to stream $all with a filter which will filter only content
// from two of the three streams. Content is filtered by prefix of the stream's ID.
func ExampleClientImpl_SubscribeToFilteredStreamAll() {
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

	prefix1 := "my_first_prefix"
	prefix2 := "my_second_prefix"

	otherStream := "read_all_existing_and_new_ones_otherStream"
	prefixStream := prefix1 + "_stream"
	newPrefixStream := prefix2 + "_stream"

	createEvents := func(count uint32) event_streams.ProposedEventList {
		result := make(event_streams.ProposedEventList, 10)
		for i := uint32(0); i < count; i++ {
			result[i] = event_streams.ProposedEvent{
				EventId:      uuid.Must(uuid.NewRandom()),
				EventType:    "TestEvent",
				ContentType:  "application/octet-stream",
				UserMetadata: []byte{},
				Data:         []byte("some event data"),
			}
		}
		return result
	}
	otherStreamEvents := createEvents(10)
	prefixStreamEvents := createEvents(10)
	newPrefixStreamEvents := createEvents(10)

	// create other stream with 10 events
	_, err := client.AppendToStream(context.Background(),
		otherStream,
		event_streams.WriteStreamRevisionNoStream{},
		otherStreamEvents)
	if err != nil {
		log.Fatalln(err)
	}

	// create first stream which content we will read
	_, err = client.AppendToStream(context.Background(),
		prefixStream,
		event_streams.WriteStreamRevisionNoStream{},
		prefixStreamEvents)
	if err != nil {
		log.Fatalln(err)
	}

	// subscribe to stream $all and filter only events written to
	// streams with prefix my_first_prefix and my_second_prefix
	streamReader, err := client.SubscribeToFilteredStreamAll(context.Background(),
		event_streams.ReadPositionAllStart{},
		false,
		event_streams.Filter{
			FilterBy: event_streams.FilterByStreamId{
				Matcher: event_streams.PrefixFilterMatcher{
					PrefixList: []string{prefix1, prefix2},
				},
			},
			Window:                       event_streams.FilterNoWindow{},
			CheckpointIntervalMultiplier: 5,
		})
	if err != nil {
		log.Fatalln(err)
	}

	waitForReadingFirstEvents := sync.WaitGroup{}
	waitForReadingFirstEvents.Add(1)

	// read events written to a stream with prefix my_first_prefix
	go func() {
		defer waitForReadingFirstEvents.Done()

		var result event_streams.ProposedEventList
		readResult, err := streamReader.ReadOne()
		if err != nil {
			log.Fatalln(err)
		}

		if event, isEvent := readResult.GetEvent(); isEvent {
			result = append(result, event.ToProposedEvent())
		}

		if reflect.DeepEqual(prefixStreamEvents, result) {
			return
		}
	}()

	waitForNewEventsAppend := sync.WaitGroup{}
	waitForNewEventsAppend.Add(1)

	// after events from stream with prefix my_first_prefix are read
	// create stream with prefix my_second_prefix
	go func() {
		defer waitForNewEventsAppend.Done()
		waitForReadingFirstEvents.Wait() // wait until all events from stream with prefix my_first_prefix are read

		// create stream with prefix my_second_prefix with 10 events in it
		_, err = client.AppendToStream(context.Background(),
			newPrefixStream,
			event_streams.WriteStreamRevisionNoStream{},
			newPrefixStreamEvents)
		if err != nil {
			log.Fatalln(err)
		}
	}()

	waitForReadingNewEvents := sync.WaitGroup{}
	waitForReadingNewEvents.Add(1)

	// read events written to a stream with prefix my_second_prefix
	go func() {
		defer waitForReadingNewEvents.Done()
		waitForNewEventsAppend.Wait() // wait until stream my_second_prefix created
		var result event_streams.ProposedEventList

		readResult, err := streamReader.ReadOne()
		if err != nil {
			log.Fatalln(err)
		}

		if event, isEvent := readResult.GetEvent(); isEvent {
			if !systemmetadata.IsSystemStream(event.Event.StreamId) {
				result = append(result, event.ToProposedEvent())
			}
		}

		// we have finished reading
		if reflect.DeepEqual(newPrefixStreamEvents, result) {
			return
		}
	}()
	// wait for reader to receive new events
	waitForReadingNewEvents.Wait()
}
