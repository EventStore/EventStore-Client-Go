package subscribe_to_stream

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/pivonroll/EventStore-Client-Go/errors"

	"github.com/pivonroll/EventStore-Client-Go/systemmetadata"

	"github.com/stretchr/testify/require"

	"github.com/pivonroll/EventStore-Client-Go/event_streams"
)

func Test_SubscribeToAll_Filtered_ReturnsCancelled(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	t.Run("Reading From Start Cancelled", func(t *testing.T) {
		wg := sync.WaitGroup{}
		wg.Add(1)

		ctx, cancelFunc := context.WithCancel(context.Background())
		streamReader, err := client.SubscribeToFilteredStreamAll(ctx,
			event_streams.ReadPositionAllStart{},
			false,
			event_streams.Filter{
				FilterBy: event_streams.FilterByStreamId{
					Matcher: event_streams.PrefixFilterMatcher{
						PrefixList: []string{"prefix", "my_prefix"},
					},
				},
				Window:                       event_streams.FilterNoWindow{},
				CheckpointIntervalMultiplier: 5,
			})
		require.NoError(t, err)

		go func() {
			defer wg.Done()

			require.Eventually(t, func() bool {
				_, err := streamReader.ReadOne()
				return err != nil && err.Code() == errors.CanceledErr
			}, time.Second*7, time.Millisecond)
		}()

		time.Sleep(1 * time.Second)
		cancelFunc()
		// wait for reader to receive cancellation
		wg.Wait()
	})

	t.Run("Reading From End Cancelled", func(t *testing.T) {
		wg := sync.WaitGroup{}
		wg.Add(1)

		ctx, cancelFunc := context.WithCancel(context.Background())
		streamReader, err := client.SubscribeToFilteredStreamAll(ctx,
			event_streams.ReadPositionAllStart{},
			false,
			event_streams.Filter{
				FilterBy: event_streams.FilterByStreamId{
					Matcher: event_streams.PrefixFilterMatcher{
						PrefixList: []string{"prefix", "my_prefix"},
					},
				},
				Window:                       event_streams.FilterNoWindow{},
				CheckpointIntervalMultiplier: 5,
			})
		require.NoError(t, err)

		go func() {
			defer wg.Done()

			require.Eventually(t, func() bool {
				_, err := streamReader.ReadOne()
				return err != nil && err.Code() == errors.CanceledErr
			}, time.Second*7, time.Second)
		}()

		time.Sleep(1 * time.Second)
		cancelFunc()
		// wait for reader to receive cancellation
		wg.Wait()
	})

	t.Run("Reading From Start Reader Closed", func(t *testing.T) {
		wg := sync.WaitGroup{}
		wg.Add(1)

		streamReader, err := client.SubscribeToFilteredStreamAll(context.Background(),
			event_streams.ReadPositionAllStart{},
			false,
			event_streams.Filter{
				FilterBy: event_streams.FilterByStreamId{
					Matcher: event_streams.PrefixFilterMatcher{
						PrefixList: []string{"prefix", "my_prefix"},
					},
				},
				Window:                       event_streams.FilterNoWindow{},
				CheckpointIntervalMultiplier: 5,
			})
		require.NoError(t, err)

		go func() {
			defer wg.Done()

			require.Eventually(t, func() bool {
				_, err := streamReader.ReadOne()
				return err != nil && err.Code() == errors.CanceledErr
			}, time.Second*7, time.Millisecond)
		}()

		time.Sleep(1 * time.Second)
		streamReader.Close()
		// wait for reader to receive stream reader's close
		wg.Wait()
	})

	t.Run("Reading From End Reader Closed", func(t *testing.T) {
		wg := sync.WaitGroup{}
		wg.Add(1)

		streamReader, err := client.SubscribeToFilteredStreamAll(context.Background(),
			event_streams.ReadPositionAllEnd{},
			false,
			event_streams.Filter{
				FilterBy: event_streams.FilterByStreamId{
					Matcher: event_streams.PrefixFilterMatcher{
						PrefixList: []string{"prefix", "my_prefix"},
					},
				},
				Window:                       event_streams.FilterNoWindow{},
				CheckpointIntervalMultiplier: 5,
			})
		require.NoError(t, err)

		go func() {
			defer wg.Done()

			require.Eventually(t, func() bool {
				_, err := streamReader.ReadOne()
				return err != nil && err.Code() == errors.CanceledErr
			}, time.Second*7, time.Millisecond)
		}()

		time.Sleep(1 * time.Second)
		streamReader.Close()
		// wait for reader to receive stream reader's close
		wg.Wait()
	})
}

func Test_SubscribeToAll_Filtered_ReadAllEventsWithPrefix(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	t.Run("Read Already Existing Events", func(t *testing.T) {
		wg := sync.WaitGroup{}
		wg.Add(1)

		prefix1 := "already_existing_first_batch"
		prefix2 := "already_existing_second_batch"
		otherStream := "already_existing_otherStream"
		prefixStream := prefix1 + "_stream"
		prefixStream2 := prefix2 + "_stream"

		otherStreamEvents := testCreateEvents(10)
		prefixStreamEvents := testCreateEvents(10)
		prefixStreamEvents2 := testCreateEvents(10)

		_, err := client.AppendToStream(context.Background(),
			otherStream,
			event_streams.WriteStreamRevisionNoStream{},
			otherStreamEvents)
		require.NoError(t, err)

		_, err = client.AppendToStream(context.Background(),
			prefixStream,
			event_streams.WriteStreamRevisionNoStream{},
			prefixStreamEvents)
		require.NoError(t, err)

		_, err = client.AppendToStream(context.Background(),
			prefixStream2,
			event_streams.WriteStreamRevisionNoStream{},
			prefixStreamEvents2)
		require.NoError(t, err)

		appPrefixEvents := append(prefixStreamEvents, prefixStreamEvents2...)

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
		require.NoError(t, err)

		go func() {
			defer wg.Done()

			var result event_streams.ProposedEventList
			require.Eventually(t, func() bool {
				readResult, err := streamReader.ReadOne()

				require.NoError(t, err)

				if event, isEvent := readResult.GetEvent(); isEvent {
					if !systemmetadata.IsSystemStream(event.Event.StreamId) {
						result = append(result, event.ToProposedEvent())
					}
				}

				return reflect.DeepEqual(appPrefixEvents, result)
			}, time.Second*10, time.Millisecond)
		}()
		// wait for reader to receive events
		wg.Wait()
	})

	t.Run("Read Already Existing Events And Keep Reading For New Ones", func(t *testing.T) {
		prefix1 := "read_all_existing_and_new_ones_already_exising_events"
		prefix2 := "read_all_existing_and_new_ones_new_events"

		otherStream := "read_all_existing_and_new_ones_otherStream"
		prefixStream := prefix1 + "_stream"
		newPrefixStream := prefix2 + "_stream"

		otherStreamEvents := testCreateEvents(10)
		prefixStreamEvents := testCreateEvents(10)
		newPrefixStreamEvents := testCreateEvents(10)

		_, err := client.AppendToStream(context.Background(),
			otherStream,
			event_streams.WriteStreamRevisionNoStream{},
			otherStreamEvents)
		require.NoError(t, err)

		_, err = client.AppendToStream(context.Background(),
			prefixStream,
			event_streams.WriteStreamRevisionNoStream{},
			prefixStreamEvents)
		require.NoError(t, err)

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
		require.NoError(t, err)

		waitForReadingFirstEvents := sync.WaitGroup{}
		waitForReadingFirstEvents.Add(1)

		// read first events
		go func() {
			defer waitForReadingFirstEvents.Done()

			var result event_streams.ProposedEventList
			require.Eventually(t, func() bool {
				readResult, err := streamReader.ReadOne()

				require.NoError(t, err)

				if event, isEvent := readResult.GetEvent(); isEvent {
					if !systemmetadata.IsSystemStream(event.Event.StreamId) {
						result = append(result, event.ToProposedEvent())
					}
				}

				return reflect.DeepEqual(prefixStreamEvents, result)
			}, time.Second*10, time.Millisecond)
		}()

		waitForNewEventsAppend := sync.WaitGroup{}
		waitForNewEventsAppend.Add(1)

		// append new events to the newPrefixStream
		go func() {
			defer waitForNewEventsAppend.Done()
			waitForReadingFirstEvents.Wait()
			_, err = client.AppendToStream(context.Background(),
				newPrefixStream,
				event_streams.WriteStreamRevisionNoStream{},
				newPrefixStreamEvents)
			require.NoError(t, err)
		}()

		waitForReadingNewEvents := sync.WaitGroup{}
		waitForReadingNewEvents.Add(1)

		// read new events
		go func() {
			defer waitForReadingNewEvents.Done()
			waitForNewEventsAppend.Wait()
			var result event_streams.ProposedEventList
			require.Eventually(t, func() bool {
				readResult, err := streamReader.ReadOne()

				require.NoError(t, err)

				if event, isEvent := readResult.GetEvent(); isEvent {
					result = append(result, event.ToProposedEvent())
				}

				return reflect.DeepEqual(newPrefixStreamEvents, result)
			}, time.Second*10, time.Millisecond)
		}()
		// wait for reader to receive new events
		waitForReadingNewEvents.Wait()
	})
}

func Test_SubscribeToAll_Filtered_WithIncorrectCredentials(t *testing.T) {
	client, closeFunc := initializeContainerAndClientWithCredentials(t,
		"wrong_user_name", "wrong_password", nil)
	defer closeFunc()

	_, err := client.SubscribeToFilteredStreamAll(context.Background(),
		event_streams.ReadPositionAllStart{},
		false,
		event_streams.Filter{
			FilterBy: event_streams.FilterByStreamId{
				Matcher: event_streams.PrefixFilterMatcher{
					PrefixList: []string{"aaa"},
				},
			},
			Window:                       event_streams.FilterNoWindow{},
			CheckpointIntervalMultiplier: 5,
		})
	require.Equal(t, errors.UnauthenticatedErr, err.Code())
}
