package client_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/pivonroll/EventStore-Client-Go/errors"
	"github.com/pivonroll/EventStore-Client-Go/event_streams"
	"github.com/stretchr/testify/require"
)

func Test_SubscribeToStream(t *testing.T) {
	container := getPrePopulatedDatabase()
	defer container.Close()
	client := createClientConnectedToContainer(container, t)
	defer func() {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}()

	t.Run("Subscribe, With Timeout, From Start To Non-Existing Stream", func(t *testing.T) {
		streamId := "subscribe_to_non_existing_stream"
		wg := sync.WaitGroup{}
		wg.Add(1)

		ctx := context.Background()
		ctx, cancelFunc := context.WithTimeout(ctx, 10*time.Second)
		defer cancelFunc()

		streamReader, err := client.EventStreams().SubscribeToStream(ctx,
			streamId,
			event_streams.SubscribeRequestOptionsStreamRevisionStart{},
			false)
		require.NoError(t, err)

		go func() {
			defer wg.Done()
			_, err := streamReader.ReadOne()
			fmt.Println(err)
			require.Equal(t, errors.DeadlineExceededErr, err.Code())
			// release lock when timeout expires
		}()

		// wait for reader to receive timeout
		wg.Wait()
	})

	t.Run("Subscription To Start Receives Canceled When Context Is Canceled", func(t *testing.T) {
		streamId := "Subscription_To_Start_Receives_Canceled_When_Context_Is_Canceled"
		wg := sync.WaitGroup{}
		wg.Add(1)

		ctx := context.Background()
		ctx, cancelFunc := context.WithTimeout(ctx, 10*time.Second)

		streamReader, err := client.EventStreams().SubscribeToStream(ctx,
			streamId,
			event_streams.SubscribeRequestOptionsStreamRevisionStart{},
			false)
		require.NoError(t, err)

		go func() {
			defer wg.Done()
			_, err := streamReader.ReadOne()
			fmt.Println(err)
			require.Equal(t, errors.CanceledErr, err.Code())
			// release lock when timeout expires
		}()

		time.Sleep(2 * time.Second)
		cancelFunc()
		// wait for reader to receive timeout
		wg.Wait()
	})

	t.Run("Subscribe From Start To Non-Existing Stream Then Get Event", func(t *testing.T) {
		streamId := "subscribe__from_start_to_non_existing_stream_then_get_event"
		wg := sync.WaitGroup{}
		wg.Add(1)

		ctx := context.Background()
		ctx, cancelFunc := context.WithTimeout(ctx, 10*time.Second)
		defer cancelFunc()

		streamReader, err := client.EventStreams().SubscribeToStream(ctx,
			streamId,
			event_streams.SubscribeRequestOptionsStreamRevisionStart{},
			false)
		require.NoError(t, err)

		go func() {
			defer wg.Done()

			response, err := streamReader.ReadOne()
			require.NoError(t, err)

			_, isEvent := response.GetEvent()
			require.True(t, isEvent)
		}()

		_, err = client.EventStreams().AppendToStream(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			testCreateEvents(1))

		wg.Wait()
	})

	t.Run("Allow Multiple Subscriptions To Same Stream From Start", func(t *testing.T) {
		streamId := "allow_multiple_subscriptions_to_same_stream_from_start"
		wg := sync.WaitGroup{}
		wg.Add(2)

		ctx := context.Background()
		ctx, cancelFunc := context.WithTimeout(ctx, 10*time.Second)
		defer cancelFunc()

		streamReader1, err := client.EventStreams().SubscribeToStream(ctx,
			streamId,
			event_streams.SubscribeRequestOptionsStreamRevisionStart{},
			false)
		require.NoError(t, err)

		streamReader2, err := client.EventStreams().SubscribeToStream(ctx,
			streamId,
			event_streams.SubscribeRequestOptionsStreamRevisionStart{},
			false)
		require.NoError(t, err)

		go func() {
			defer wg.Done()

			response, err := streamReader1.ReadOne()
			require.NoError(t, err)

			_, isEvent := response.GetEvent()
			require.True(t, isEvent)
		}()

		go func() {
			defer wg.Done()

			response, err := streamReader2.ReadOne()
			require.NoError(t, err)

			_, isEvent := response.GetEvent()
			require.True(t, isEvent)
		}()

		_, err = client.EventStreams().AppendToStream(context.Background(),
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

		_, err := client.EventStreams().AppendToStream(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			beforeEvents)
		require.NoError(t, err)

		streamReader, err := client.EventStreams().SubscribeToStream(ctx,
			streamId,
			event_streams.SubscribeRequestOptionsStreamRevisionStart{},
			false)
		require.NoError(t, err)

		go func() {
			defer readerWait.Done()

			var result []event_streams.ProposedEvent

			for {
				response, err := streamReader.ReadOne()
				if err != nil {
					if err.Code() == errors.CanceledErr {
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

		_, err = client.EventStreams().AppendToStream(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevisionAny{},
			afterEvents)

		cancelWait.Wait()
		cancelFunc()

		readerWait.Wait()
	})

	t.Run("Subscription to Start Catches Deletions", func(t *testing.T) {
		streamId := "subscription_to_start_catches_deletions"

		wg := sync.WaitGroup{}
		wg.Add(1)

		ctx := context.Background()
		ctx, cancelFunc := context.WithTimeout(ctx, 10*time.Second)
		defer cancelFunc()

		streamReader, err := client.EventStreams().SubscribeToStream(ctx,
			streamId,
			event_streams.SubscribeRequestOptionsStreamRevisionStart{},
			false)
		require.NoError(t, err)

		go func() {
			defer wg.Done()
			fmt.Println("Reading a stream")
			_, err := streamReader.ReadOne()
			require.Equal(t, errors.StreamDeletedErr, err.Code())
			// release lock when timeout expires
		}()

		time.Sleep(2 * time.Second)
		_, err = client.EventStreams().TombstoneStream(context.Background(),
			streamId,
			event_streams.TombstoneRequestExpectedStreamRevisionNoStream{})
		require.NoError(t, err)
		// wait for reader to receive timeout
		wg.Wait()
	})

	t.Run("Does Not Read Existing Events But Keep Listening To New Ones", func(t *testing.T) {
		streamId := "does_not_read_existing_events_but_keep_listening_to_new_ones"
		readerWait := sync.WaitGroup{}
		readerWait.Add(1)

		cancelWait := sync.WaitGroup{}
		cancelWait.Add(1)

		ctx := context.Background()
		ctx, cancelFunc := context.WithTimeout(ctx, 20*time.Second)

		beforeEvents := testCreateEvents(3)
		afterEvents := testCreateEvents(2)

		_, err := client.EventStreams().AppendToStream(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			beforeEvents)
		require.NoError(t, err)

		streamReader, err := client.EventStreams().SubscribeToStream(ctx,
			streamId,
			event_streams.SubscribeRequestOptionsStreamRevisionEnd{},
			false)
		require.NoError(t, err)

		go func() {
			defer readerWait.Done()

			var result []event_streams.ProposedEvent

			for {
				response, err := streamReader.ReadOne()
				if err != nil {
					if err.Code() == errors.CanceledErr {
						break
					}
					cancelWait.Done()
					t.Fail()
				}

				require.NoError(t, err)

				event, isEvent := response.GetEvent()
				require.True(t, isEvent)
				result = append(result, event.ToProposedEvent())
				if len(result) == len(afterEvents) {
					cancelWait.Done()
				}
			}

			require.Equal(t, afterEvents, result)
		}()

		_, err = client.EventStreams().AppendToStream(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevisionAny{},
			afterEvents)

		cancelWait.Wait()
		cancelFunc()

		readerWait.Wait()
	})

	t.Run("Subscribe To Non Existing Stream And Then Catch New Event", func(t *testing.T) {
		streamId := "subscribe_to_non_existing_stream_and_then_catch_new_event"
		wg := sync.WaitGroup{}
		wg.Add(1)

		ctx := context.Background()
		ctx, cancelFunc := context.WithTimeout(ctx, 10*time.Second)
		defer cancelFunc()

		streamReader, err := client.EventStreams().SubscribeToStream(ctx,
			streamId,
			event_streams.SubscribeRequestOptionsStreamRevisionEnd{},
			false)
		require.NoError(t, err)

		go func() {
			defer wg.Done()

			response, err := streamReader.ReadOne()
			require.NoError(t, err)

			_, isEvent := response.GetEvent()
			require.True(t, isEvent)
		}()

		_, err = client.EventStreams().AppendToStream(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			testCreateEvents(1))

		wg.Wait()
	})

	t.Run("Allow Multiple Subscriptions To Same Stream From End", func(t *testing.T) {
		streamId := "allow_multiple_subscriptions_to_same_stream_from_end"
		wg := sync.WaitGroup{}
		wg.Add(2)

		ctx := context.Background()
		ctx, cancelFunc := context.WithTimeout(ctx, 10*time.Second)
		defer cancelFunc()

		streamReader1, err := client.EventStreams().SubscribeToStream(ctx,
			streamId,
			event_streams.SubscribeRequestOptionsStreamRevisionEnd{},
			false)
		require.NoError(t, err)

		streamReader2, err := client.EventStreams().SubscribeToStream(ctx,
			streamId,
			event_streams.SubscribeRequestOptionsStreamRevisionEnd{},
			false)
		require.NoError(t, err)

		go func() {
			defer wg.Done()

			response, err := streamReader1.ReadOne()
			require.NoError(t, err)

			_, isEvent := response.GetEvent()
			require.True(t, isEvent)
		}()

		go func() {
			defer wg.Done()

			response, err := streamReader2.ReadOne()
			require.NoError(t, err)

			_, isEvent := response.GetEvent()
			require.True(t, isEvent)
		}()

		_, err = client.EventStreams().AppendToStream(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			testCreateEvents(1))

		wg.Wait()
	})

	t.Run("Subscription To End Receives Canceled When Context Is Canceled", func(t *testing.T) {
		streamId := "Subscription_To_End_Receives_Canceled_When_Context_Is_Canceled"
		wg := sync.WaitGroup{}
		wg.Add(1)

		ctx := context.Background()
		ctx, cancelFunc := context.WithTimeout(ctx, 10*time.Second)

		streamReader, err := client.EventStreams().SubscribeToStream(ctx,
			streamId,
			event_streams.SubscribeRequestOptionsStreamRevisionEnd{},
			false)
		require.NoError(t, err)

		go func() {
			defer wg.Done()
			_, err := streamReader.ReadOne()
			fmt.Println(err)
			require.Equal(t, errors.CanceledErr, err.Code())
			// release lock when timeout expires
		}()

		time.Sleep(2 * time.Second)
		cancelFunc()
		// wait for reader to receive timeout
		wg.Wait()
	})

	t.Run("Subscription to End Catches Deletions", func(t *testing.T) {
		streamId := "subscription_to_end_catches_deletions"

		wg := sync.WaitGroup{}
		wg.Add(1)

		ctx := context.Background()
		ctx, cancelFunc := context.WithTimeout(ctx, 10*time.Second)
		defer cancelFunc()

		streamReader, err := client.EventStreams().SubscribeToStream(ctx,
			streamId,
			event_streams.SubscribeRequestOptionsStreamRevisionEnd{},
			false)
		require.NoError(t, err)

		go func() {
			defer wg.Done()
			fmt.Println("Reading a stream")
			_, err := streamReader.ReadOne()
			require.Equal(t, errors.StreamDeletedErr, err.Code())
			// release lock when timeout expires
		}()

		time.Sleep(2 * time.Second)
		_, err = client.EventStreams().TombstoneStream(context.Background(),
			streamId,
			event_streams.TombstoneRequestExpectedStreamRevisionNoStream{})
		require.NoError(t, err)
		// wait for reader to receive timeout
		wg.Wait()
	})

	t.Run("Subscribe, With Timeout, From Specific Revision To Non-Existing Stream", func(t *testing.T) {
		streamId := "subscribe_from_specific_revision_to_non_existing_stream"
		wg := sync.WaitGroup{}
		wg.Add(1)

		ctx := context.Background()
		ctx, cancelFunc := context.WithTimeout(ctx, 5*time.Second)
		defer cancelFunc()

		streamReader, err := client.EventStreams().SubscribeToStream(ctx,
			streamId,
			event_streams.SubscribeRequestOptionsStreamRevision{Revision: 2},
			false)
		require.NoError(t, err)

		go func() {
			defer wg.Done()
			_, err := streamReader.ReadOne()
			fmt.Println(err)
			require.Equal(t, errors.DeadlineExceededErr, err.Code())
			// release lock when timeout expires
		}()

		// wait for reader to receive timeout
		wg.Wait()
	})

	t.Run("Subscribe From Specific Revision To Non-Existing Stream Then Get Event", func(t *testing.T) {
		streamId := "subscribe__from_start_to_non_existing_stream_then_get_event"
		wg := sync.WaitGroup{}
		wg.Add(1)

		ctx := context.Background()
		ctx, cancelFunc := context.WithTimeout(ctx, 5*time.Second)
		defer cancelFunc()

		streamReader, err := client.EventStreams().SubscribeToStream(ctx,
			streamId,
			event_streams.SubscribeRequestOptionsStreamRevision{Revision: 0},
			false)
		require.NoError(t, err)

		firstEvent := testCreateEvent()
		secondEvent := testCreateEvent()

		go func() {
			defer wg.Done()

			response, err := streamReader.ReadOne()
			require.NoError(t, err)

			event, isEvent := response.GetEvent()
			require.True(t, isEvent)
			require.Equal(t, secondEvent, event.ToProposedEvent())
		}()

		_, err = client.EventStreams().AppendToStream(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			[]event_streams.ProposedEvent{firstEvent})
		require.NoError(t, err)

		_, err = client.EventStreams().AppendToStream(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevisionAny{},
			[]event_streams.ProposedEvent{secondEvent})
		require.NoError(t, err)

		wg.Wait()
	})

	t.Run("Allow Multiple Subscriptions To Same Stream From Specific Revision", func(t *testing.T) {
		streamId := "allow_multiple_subscriptions_to_same_stream_from_specific_revision"
		wg := sync.WaitGroup{}
		wg.Add(2)

		ctx := context.Background()
		ctx, cancelFunc := context.WithTimeout(ctx, 10*time.Second)
		defer cancelFunc()

		streamReader1, err := client.EventStreams().SubscribeToStream(ctx,
			streamId,
			event_streams.SubscribeRequestOptionsStreamRevision{Revision: 0},
			false)
		require.NoError(t, err)

		streamReader2, err := client.EventStreams().SubscribeToStream(ctx,
			streamId,
			event_streams.SubscribeRequestOptionsStreamRevision{Revision: 0},
			false)
		require.NoError(t, err)

		expectedEvent := testCreateEvent()

		go func() {
			defer wg.Done()

			response, err := streamReader1.ReadOne()
			require.NoError(t, err)

			event, isEvent := response.GetEvent()
			require.True(t, isEvent)
			require.Equal(t, expectedEvent, event.ToProposedEvent())
		}()

		go func() {
			defer wg.Done()

			response, err := streamReader2.ReadOne()
			require.NoError(t, err)

			event, isEvent := response.GetEvent()
			require.True(t, isEvent)
			require.Equal(t, expectedEvent, event.ToProposedEvent())
		}()

		_, err = client.EventStreams().AppendToStream(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			testCreateEvents(1))
		require.NoError(t, err)

		_, err = client.EventStreams().AppendToStream(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevisionAny{},
			[]event_streams.ProposedEvent{expectedEvent})
		require.NoError(t, err)

		wg.Wait()
	})

	t.Run("Subscription To Specific Revision Receives Canceled When Context Is Canceled", func(t *testing.T) {
		streamId := "Subscription_To_Specific_Revision_Receives_Canceled_When_Context_Is_Canceled"
		wg := sync.WaitGroup{}
		wg.Add(1)

		ctx := context.Background()
		ctx, cancelFunc := context.WithTimeout(ctx, 10*time.Second)

		streamReader, err := client.EventStreams().SubscribeToStream(ctx,
			streamId,
			event_streams.SubscribeRequestOptionsStreamRevision{Revision: 1},
			false)
		require.NoError(t, err)

		go func() {
			defer wg.Done()
			_, err := streamReader.ReadOne()
			fmt.Println(err)
			require.Equal(t, errors.CanceledErr, err.Code())
			// release lock when timeout expires
		}()

		time.Sleep(1 * time.Second)
		cancelFunc()
		// wait for reader to receive timeout
		wg.Wait()
	})
}

func TestStreamSubscriptionDeliversAllEventsInStreamAndListensForNewEvents(t *testing.T) {
	container := getPrePopulatedDatabase()
	defer container.Close()
	client := createClientConnectedToContainer(container, t)
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
	subscription, err := client.EventStreams().SubscribeToStream(
		context.Background(),
		"dataset20M-0",
		event_streams.SubscribeRequestOptionsStreamRevisionStart{},
		false)
	require.NoError(t, err)

	fmt.Println("Subscribed")
	go func() {
		current := 0
		for {
			subEvent, err := subscription.ReadOne()
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
	writeResult, err := client.EventStreams().AppendToStream(context.Background(),
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
