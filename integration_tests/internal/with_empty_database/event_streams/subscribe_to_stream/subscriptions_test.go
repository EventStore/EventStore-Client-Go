package subscribe_to_stream

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pivonroll/EventStore-Client-Go/errors"
	"github.com/pivonroll/EventStore-Client-Go/event_streams"
	"github.com/stretchr/testify/require"
)

func Test_SubscribeToStream(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	t.Run("Subscribe, With Timeout, From Start To Non-Existing Stream", func(t *testing.T) {
		streamId := "subscribe_with_timeout_from_start_to_non_existing_stream"
		wg := sync.WaitGroup{}
		wg.Add(1)

		ctx := context.Background()
		ctx, cancelFunc := context.WithTimeout(ctx, 10*time.Second)
		defer cancelFunc()

		streamReader, err := client.SubscribeToStream(ctx,
			streamId,
			event_streams.ReadStreamRevisionStart{},
			false)
		require.NoError(t, err)

		go func() {
			defer wg.Done()
			_, err := streamReader.ReadOne()
			require.Equal(t, errors.DeadlineExceededErr, err.Code())

			_, err = streamReader.ReadOne()
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

		ctx, cancelFunc := context.WithCancel(context.Background())

		streamReader, err := client.SubscribeToStream(ctx,
			streamId,
			event_streams.ReadStreamRevisionStart{},
			false)
		require.NoError(t, err)

		go func() {
			defer wg.Done()
			_, err := streamReader.ReadOne()
			require.Equal(t, errors.CanceledErr, err.Code())

			_, err = streamReader.ReadOne()
			require.Equal(t, errors.CanceledErr, err.Code())
			// release lock when timeout expires
		}()

		time.Sleep(2 * time.Second)
		cancelFunc()
		// wait for reader to receive cancellation
		wg.Wait()
	})

	t.Run("Subscribe From Start To Non-Existing Stream Then Get Event", func(t *testing.T) {
		streamId := "subscribe__from_start_to_non_existing_stream_then_get_event"
		wg := sync.WaitGroup{}
		wg.Add(1)

		ctx := context.Background()
		ctx, cancelFunc := context.WithTimeout(ctx, 10*time.Second)
		defer cancelFunc()

		streamReader, err := client.SubscribeToStream(ctx,
			streamId,
			event_streams.ReadStreamRevisionStart{},
			false)
		require.NoError(t, err)

		go func() {
			defer wg.Done()

			response, err := streamReader.ReadOne()
			require.NoError(t, err)

			_, isEvent := response.GetEvent()
			require.True(t, isEvent)
		}()

		_, err = client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionNoStream{},
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

		streamReader1, err := client.SubscribeToStream(ctx,
			streamId,
			event_streams.ReadStreamRevisionStart{},
			false)
		require.NoError(t, err)

		streamReader2, err := client.SubscribeToStream(ctx,
			streamId,
			event_streams.ReadStreamRevisionStart{},
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

		_, err = client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionNoStream{},
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
			event_streams.WriteStreamRevisionNoStream{},
			beforeEvents)
		require.NoError(t, err)

		streamReader, err := client.SubscribeToStream(ctx,
			streamId,
			event_streams.ReadStreamRevisionStart{},
			false)
		require.NoError(t, err)

		go func() {
			defer readerWait.Done()

			var result event_streams.ProposedEventList

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

			require.Equal(t, totalEvents, result)
		}()

		_, err = client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionAny{},
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

		streamReader, err := client.SubscribeToStream(ctx,
			streamId,
			event_streams.ReadStreamRevisionStart{},
			false)
		require.NoError(t, err)

		go func() {
			defer wg.Done()
			_, err := streamReader.ReadOne()
			require.Equal(t, errors.StreamDeletedErr, err.Code())
		}()

		_, err = client.TombstoneStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionNoStream{})
		require.NoError(t, err)
		// wait for reader to receive tombstone
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

		_, err := client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionNoStream{},
			beforeEvents)
		require.NoError(t, err)

		streamReader, err := client.SubscribeToStream(ctx,
			streamId,
			event_streams.ReadStreamRevisionEnd{},
			false)
		require.NoError(t, err)

		go func() {
			defer readerWait.Done()

			var result event_streams.ProposedEventList

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

		_, err = client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionAny{},
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

		streamReader, err := client.SubscribeToStream(ctx,
			streamId,
			event_streams.ReadStreamRevisionEnd{},
			false)
		require.NoError(t, err)

		go func() {
			defer wg.Done()

			response, err := streamReader.ReadOne()
			require.NoError(t, err)

			_, isEvent := response.GetEvent()
			require.True(t, isEvent)
		}()

		_, err = client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionNoStream{},
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

		streamReader1, err := client.SubscribeToStream(ctx,
			streamId,
			event_streams.ReadStreamRevisionEnd{},
			false)
		require.NoError(t, err)

		streamReader2, err := client.SubscribeToStream(ctx,
			streamId,
			event_streams.ReadStreamRevisionEnd{},
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

		_, err = client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionNoStream{},
			testCreateEvents(1))

		wg.Wait()
	})

	t.Run("Subscription To End Receives Canceled When Context Is Canceled", func(t *testing.T) {
		streamId := "Subscription_To_End_Receives_Canceled_When_Context_Is_Canceled"
		wg := sync.WaitGroup{}
		wg.Add(1)

		ctx := context.Background()
		ctx, cancelFunc := context.WithTimeout(ctx, 10*time.Second)

		streamReader, err := client.SubscribeToStream(ctx,
			streamId,
			event_streams.ReadStreamRevisionEnd{},
			false)
		require.NoError(t, err)

		go func() {
			defer wg.Done()
			_, err := streamReader.ReadOne()
			require.Equal(t, errors.CanceledErr, err.Code())
			// release lock when timeout expires
		}()

		time.Sleep(2 * time.Second)
		cancelFunc()
		// wait for reader to receive cancellation
		wg.Wait()
	})

	t.Run("Subscription to End Catches Deletions", func(t *testing.T) {
		streamId := "subscription_to_end_catches_deletions"

		wg := sync.WaitGroup{}
		wg.Add(1)

		ctx := context.Background()
		ctx, cancelFunc := context.WithTimeout(ctx, 10*time.Second)
		defer cancelFunc()

		streamReader, err := client.SubscribeToStream(ctx,
			streamId,
			event_streams.ReadStreamRevisionEnd{},
			false)
		require.NoError(t, err)

		go func() {
			defer wg.Done()
			_, err := streamReader.ReadOne()
			require.Equal(t, errors.StreamDeletedErr, err.Code())
			// release lock when timeout expires
		}()

		time.Sleep(2 * time.Second)
		_, err = client.TombstoneStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionNoStream{})
		require.NoError(t, err)
		// wait for reader to receive tombstone
		wg.Wait()
	})

	t.Run("Subscribe, With Timeout, From Specific Revision To Non-Existing Stream", func(t *testing.T) {
		streamId := "subscribe_from_specific_revision_to_non_existing_stream"
		wg := sync.WaitGroup{}
		wg.Add(1)

		ctx := context.Background()
		ctx, cancelFunc := context.WithTimeout(ctx, 5*time.Second)
		defer cancelFunc()

		streamReader, err := client.SubscribeToStream(ctx,
			streamId,
			event_streams.ReadStreamRevision{Revision: 2},
			false)
		require.NoError(t, err)

		go func() {
			defer wg.Done()
			_, err := streamReader.ReadOne()
			require.Equal(t, errors.DeadlineExceededErr, err.Code())
			// release lock when timeout expires
		}()

		// wait for reader to receive timeout
		wg.Wait()
	})

	t.Run("Subscribe From Specific Revision To Non-Existing Stream Then Get Event", func(t *testing.T) {
		streamId := "subscribe__from_specific_revision_to_non_existing_stream_then_get_event"
		wg := sync.WaitGroup{}
		wg.Add(1)

		ctx := context.Background()
		ctx, cancelFunc := context.WithTimeout(ctx, 5*time.Second)
		defer cancelFunc()

		streamReader, err := client.SubscribeToStream(ctx,
			streamId,
			event_streams.ReadStreamRevision{Revision: 0},
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

		_, err = client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionNoStream{},
			[]event_streams.ProposedEvent{firstEvent})
		require.NoError(t, err)

		_, err = client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionAny{},
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

		streamReader1, err := client.SubscribeToStream(ctx,
			streamId,
			event_streams.ReadStreamRevision{Revision: 0},
			false)
		require.NoError(t, err)

		streamReader2, err := client.SubscribeToStream(ctx,
			streamId,
			event_streams.ReadStreamRevision{Revision: 0},
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

		_, err = client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionNoStream{},
			testCreateEvents(1))
		require.NoError(t, err)

		_, err = client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionAny{},
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

		streamReader, err := client.SubscribeToStream(ctx,
			streamId,
			event_streams.ReadStreamRevision{Revision: 1},
			false)
		require.NoError(t, err)

		go func() {
			defer wg.Done()
			_, err := streamReader.ReadOne()
			require.Equal(t, errors.CanceledErr, err.Code())
			// release lock when timeout expires
		}()

		time.Sleep(1 * time.Second)
		cancelFunc()
		// wait for reader to receive cancellation
		wg.Wait()
	})
}

func Test_SubscribeToStream_WithIncorrectCredentials(t *testing.T) {
	client, closeFunc := initializeContainerAndClientWithCredentials(t,
		"wrong_user_name", "wrong_password", nil)
	defer closeFunc()

	streamId := "stream_does_not_exist_throws"
	_, err := client.SubscribeToStream(context.Background(),
		streamId,
		event_streams.ReadStreamRevisionStart{},
		false)
	require.Equal(t, errors.UnauthenticatedErr, err.Code())
}
