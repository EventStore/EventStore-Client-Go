package client_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"sync"
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/event_streams"
	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/require"
)

func TestStreamSubscriptionDeliversAllEventsInStreamAndListensForNewEvents(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()
	client := CreateTestClient(container, t)
	defer func() {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}()

	streamID := "dataset20M-0"
	testEvent := event_streams.ProposedEvent{
		EventID:      uuid.FromStringOrNil("84c8e36c-4e64-11ea-8b59-b7f658acfc9f"),
		EventType:    "TestEvent",
		ContentType:  "application/octet-stream",
		UserMetadata: []byte{0xd, 0xe, 0xa, 0xd},
		Data:         []byte{0xb, 0xe, 0xe, 0xf},
	}

	var receivedEvents sync.WaitGroup
	var appendedEvents sync.WaitGroup
	fmt.Println("Subscribing...")
	subscription, err := client.SubscribeToStream(
		context.Background(),
		"dataset20M-0",
		event_streams.SubscribeRequestOptionsStreamRevisionStart{},
		false)
	require.NoError(t, err)

	fmt.Println("Subscribed")
	go func() {
		current := 0
		for {
			fmt.Println("Receiving...")
			subEvent, err := subscription.Recv()
			require.NoError(t, err)
			fmt.Println("Processing: ", current)

			if event, isEvent := subEvent.GetEvent(); isEvent {
				fmt.Println("Is event")
				current++
				if current <= 6_000 {
					receivedEvents.Done()
					continue
				}

				require.Equal(t, testEvent.EventID, event.Event.Id)
				require.Equal(t, uint64(6_000), event.Event.StreamRevision)
				require.Equal(t, streamID, event.Event.StreamIdentifier)
				require.Equal(t, testEvent.Data, event.Event.Data)
				require.Equal(t, testEvent.UserMetadata, event.Event.CustomMetadata)
				appendedEvents.Done()
				break
			}
		}
	}()

	receivedEvents.Add(6_000)
	appendedEvents.Add(1)
	fmt.Println("Waiting 1...")
	timedOut := waitWithTimeout(&receivedEvents, time.Duration(5)*time.Second)
	require.False(t, timedOut, "Timed out waiting for initial set of events")

	// Write a new event
	writeResult, err := client.AppendToStream(context.Background(),
		streamID,
		event_streams.AppendRequestExpectedStreamRevision{Revision: 5999},
		[]event_streams.ProposedEvent{testEvent})
	require.NoError(t, err)
	require.Equal(t, uint64(6_000), writeResult.NextExpectedVersion)

	// Assert event was forwarded to the subscription
	fmt.Println("Waiting 2...")
	timedOut = waitWithTimeout(&appendedEvents, time.Duration(5)*time.Second)
	require.False(t, timedOut, "Timed out waiting for the appended events")
	defer subscription.Close()
}

type Position struct {
	Prepare uint64 `json:"prepare"`
	Commit  uint64 `json:"commit"`
}

func TestAllSubscriptionWithFilterDeliversCorrectEvents(t *testing.T) {
	positionsContent, err := ioutil.ReadFile("../resources/test/all-positions-filtered-stream-194-e0-e30.json")
	require.NoError(t, err)
	versionsContent, err := ioutil.ReadFile("../resources/test/all-versions-filtered-stream-194-e0-e30.json")
	require.NoError(t, err)
	var positions []Position
	var versions []uint64
	err = json.Unmarshal(positionsContent, &positions)
	require.NoError(t, err)
	err = json.Unmarshal(versionsContent, &versions)
	require.NoError(t, err)

	container := GetPrePopulatedDatabase()
	defer container.Close()
	client := CreateTestClient(container, t)

	defer func() {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}()

	var receivedEvents sync.WaitGroup
	receivedEvents.Add(len(positions))

	filter := event_streams.SubscribeRequestFilter{
		FilterBy: event_streams.SubscribeRequestFilterByEventType{
			Regex:  "",
			Prefix: []string{"eventType-194"},
		},
		Window:                       event_streams.SubscribeRequestFilterWindowMax{Max: 32},
		CheckpointIntervalMultiplier: 1,
	}

	subscription, err := client.SubscribeToAllFiltered(
		context.Background(),
		event_streams.SubscribeRequestOptionsAllStartPosition{},
		false,
		filter)
	require.NoError(t, err)

	go func() {
		current := 0
		for {
			subEvent, err := subscription.Recv()
			require.NoError(t, err)

			//if subEvent.Dropped != nil {
			//	break
			//}

			if event, isEvent := subEvent.GetEvent(); isEvent {
				require.Equal(t, versions[current], event.Event.StreamRevision)
				require.Equal(t, positions[current].Commit, event.Event.CommitPosition)
				require.Equal(t, positions[current].Prepare, event.Event.PreparePosition)
				current++
				receivedEvents.Done()
			}
		}
	}()

	timedOut := waitWithTimeout(&receivedEvents, time.Duration(20)*time.Second)
	require.False(t, timedOut, "Timed out while waiting for events via the subscription")
}

//func TestConnectionClosing(t *testing.T) {
//	container := GetPrePopulatedDatabase()
//	defer container.Close()
//	client := CreateTestClient(container, t)
//	defer client.Close()
//
//	var receivedEvents sync.WaitGroup
//	var droppedEvent sync.WaitGroup
//	subscription, err := client.SubscribeToStream_OLD(context.Background(), "dataset20M-0", stream_position.Start{}, false)
//
//	go func() {
//		current := 1
//
//		for {
//			subEvent := subscription.Recv()
//
//			if subEvent.EventAppeared != nil {
//				if current <= 10 {
//					receivedEvents.Done()
//					current++
//				}
//
//				continue
//			}
//
//			if subEvent.Dropped != nil {
//				droppedEvent.Done()
//				break
//			}
//		}
//	}()
//
//	require.NoError(t, err)
//	receivedEvents.Add(10)
//	droppedEvent.Add(1)
//	timedOut := waitWithTimeout(&receivedEvents, time.Duration(5)*time.Second)
//	require.False(t, timedOut, "Timed out waiting for initial set of events")
//	subscription.Close()
//	timedOut = waitWithTimeout(&droppedEvent, time.Duration(5)*time.Second)
//	require.False(t, timedOut, "Timed out waiting for dropped event")
//}

func waitWithTimeout(wg *sync.WaitGroup, duration time.Duration) bool {
	channel := make(chan struct{})
	go func() {
		defer close(channel)
		wg.Wait()
	}()
	select {
	case <-channel:
		return false
	case <-time.After(duration):
		return true
	}
}
