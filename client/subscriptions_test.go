package client_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"sync"
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/errors"
	"github.com/EventStore/EventStore-Client-Go/event_streams"
	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/require"
)

func Test_SubscribeToStream(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()
	client := CreateTestClient(container, t)
	defer func() {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}()

	t.Run("Subscribe To Non-Existing Stream", func(t *testing.T) {
		streamId := "subscribe_to_non_existing_stream"
		wg := sync.WaitGroup{}
		wg.Add(1)

		ctx := context.Background()
		ctx, cancelFunc := context.WithTimeout(ctx, 10*time.Second)
		defer cancelFunc()

		streamReader, err := client.SubscribeToStream(ctx,
			streamId,
			event_streams.SubscribeRequestOptionsStreamRevisionStart{},
			false)
		require.NoError(t, err)

		go func() {
			defer wg.Done()
			_, err := streamReader.Recv()
			fmt.Println(err)
			require.Equal(t, errors.DeadlineExceededErr, err.Code())
			// release lock when timeout expires
		}()

		// wait for reader to receive timeout
		wg.Wait()
	})

	t.Run("Receive Canceled Error When Canceled ", func(t *testing.T) {
		streamId := "calls_subscription_dropped_when_disposed"
		wg := sync.WaitGroup{}
		wg.Add(1)

		ctx := context.Background()
		ctx, cancelFunc := context.WithTimeout(ctx, 10*time.Second)

		streamReader, err := client.SubscribeToStream(ctx,
			streamId,
			event_streams.SubscribeRequestOptionsStreamRevisionStart{},
			false)
		require.NoError(t, err)

		go func() {
			defer wg.Done()
			_, err := streamReader.Recv()
			fmt.Println(err)
			require.Equal(t, errors.CanceledErr, err.Code())
			// release lock when timeout expires
		}()

		time.Sleep(2 * time.Second)
		cancelFunc()
		// wait for reader to receive timeout
		wg.Wait()
	})

	t.Run("Subscribe To Non-Existing Stream Then Get Event", func(t *testing.T) {
		streamId := "subscribe_to_non_existing_stream_then_get_event"
		wg := sync.WaitGroup{}
		wg.Add(1)

		ctx := context.Background()
		ctx, cancelFunc := context.WithTimeout(ctx, 10*time.Second)
		defer cancelFunc()

		streamReader, err := client.SubscribeToStream(ctx,
			streamId,
			event_streams.SubscribeRequestOptionsStreamRevisionStart{},
			false)
		require.NoError(t, err)

		go func() {
			defer wg.Done()

			response, err := streamReader.Recv()
			require.NoError(t, err)

			_, isEvent := response.GetEvent()
			require.True(t, isEvent)
		}()

		_, err = client.AppendToStream(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			testCreateEvents(1))

		wg.Wait()
	})

	t.Run("Allow Multiple Subscriptions To Same Stream", func(t *testing.T) {
		streamId := "allow_multiple_subscriptions_to_same_stream"
		wg := sync.WaitGroup{}
		wg.Add(2)

		ctx := context.Background()
		ctx, cancelFunc := context.WithTimeout(ctx, 10*time.Second)
		defer cancelFunc()

		streamReader1, err := client.SubscribeToStream(ctx,
			streamId,
			event_streams.SubscribeRequestOptionsStreamRevisionStart{},
			false)
		require.NoError(t, err)

		streamReader2, err := client.SubscribeToStream(ctx,
			streamId,
			event_streams.SubscribeRequestOptionsStreamRevisionStart{},
			false)
		require.NoError(t, err)

		go func() {
			defer wg.Done()

			response, err := streamReader1.Recv()
			require.NoError(t, err)

			_, isEvent := response.GetEvent()
			require.True(t, isEvent)
		}()

		go func() {
			defer wg.Done()

			response, err := streamReader2.Recv()
			require.NoError(t, err)

			_, isEvent := response.GetEvent()
			require.True(t, isEvent)
		}()

		_, err = client.AppendToStream(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			testCreateEvents(1))

		wg.Wait()
	})

	t.Run("Reads All Existing Events And Keeps Listening To New Ones", func(t *testing.T) {
		streamId := "reads_all_existing_events_and_keep_listening_to_new_ones"
		readerWait := sync.WaitGroup{}
		readerWait.Add(1)

		cancelWait := sync.WaitGroup{}
		cancelWait.Add(1)

		ctx := context.Background()
		ctx, cancelFunc := context.WithTimeout(ctx, 20*time.Second)

		beforeEvents := testCreateEvents(3)
		afterEvents := testCreateEvents(2)
		totalEvents := append(beforeEvents, afterEvents...)

		_, err := client.AppendToStream(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			beforeEvents)
		require.NoError(t, err)

		streamReader, err := client.SubscribeToStream(ctx,
			streamId,
			event_streams.SubscribeRequestOptionsStreamRevisionStart{},
			false)
		require.NoError(t, err)

		go func() {
			defer readerWait.Done()

			var result []event_streams.ProposedEvent

			for {
				response, err := streamReader.Recv()
				if err != nil {
					if err.Code() == event_streams.EndOfStreamErr ||
						err.Code() == errors.CanceledErr {
						break
					}
					cancelWait.Done()
					t.Fail()
				}

				require.NoError(t, err)

				event, isEvent := response.GetEvent()
				require.True(t, isEvent)
				result = append(result, event.ToProposedEvent())
				if len(result) == len(totalEvents) {
					cancelWait.Done()
				}
			}

			require.Equal(t, append(beforeEvents, afterEvents...), result)
		}()

		_, err = client.AppendToStream(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevisionAny{},
			afterEvents)

		cancelWait.Wait()
		cancelFunc()

		readerWait.Wait()
	})

	t.Run("catches_deletions", func(t *testing.T) {
		streamId := "catches_deletions"

		wg := sync.WaitGroup{}
		wg.Add(1)

		ctx := context.Background()
		ctx, cancelFunc := context.WithTimeout(ctx, 10*time.Second)
		defer cancelFunc()

		streamReader, err := client.SubscribeToStream(ctx,
			streamId,
			event_streams.SubscribeRequestOptionsStreamRevisionStart{},
			false)
		require.NoError(t, err)

		go func() {
			defer wg.Done()
			_, err := streamReader.Recv()
			fmt.Println(err)
			require.Equal(t, errors.StreamDeletedErr, err.Code())
			// release lock when timeout expires
		}()

		time.Sleep(2 * time.Second)
		_, err = client.TombstoneStream(context.Background(),
			streamId,
			event_streams.TombstoneRequestExpectedStreamRevisionNoStream{})
		require.NoError(t, err)
		// wait for reader to receive timeout
		wg.Wait()
	})
}

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
			subEvent, err := subscription.Recv()
			require.NoError(t, err)

			if event, isEvent := subEvent.GetEvent(); isEvent {
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
	success, _ := writeResult.GetSuccess()
	require.Equal(t, uint64(6_000), success.GetCurrentRevision())

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
