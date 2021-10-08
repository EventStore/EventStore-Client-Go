package subscribe_to_stream

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/pivonroll/EventStore-Client-Go/systemmetadata"

	"github.com/pivonroll/EventStore-Client-Go/errors"

	"github.com/stretchr/testify/require"

	"github.com/pivonroll/EventStore-Client-Go/event_streams"
)

func Test_SubscribeToAll_FromStart_ReturnsSubscriptionDroppedWhenCancelled(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	wg := sync.WaitGroup{}
	wg.Add(1)

	ctx, cancelFunc := context.WithCancel(context.Background())
	streamReader, err := client.SubscribeToStreamAll(ctx,
		event_streams.ReadPositionAllStart{},
		false)
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
}

func Test_SubscribeToAll_FromEnd_ReturnsSubscriptionDroppedWhenCancelled(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	wg := sync.WaitGroup{}
	wg.Add(1)

	ctx, cancelFunc := context.WithCancel(context.Background())
	streamReader, err := client.SubscribeToStreamAll(ctx,
		event_streams.ReadPositionAllEnd{},
		false)
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
}

func Test_SubscribeToAll_FromStart_ReturnsSubscriptionDroppedWhenReaderClosed(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	wg := sync.WaitGroup{}
	wg.Add(1)

	streamReader, err := client.SubscribeToStreamAll(context.Background(),
		event_streams.ReadPositionAllStart{},
		false)
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
}

func Test_SubscribeToAll_FromEnd_ReturnsSubscriptionDroppedWhenReaderClosed(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	wg := sync.WaitGroup{}
	wg.Add(1)

	streamReader, err := client.SubscribeToStreamAll(context.Background(),
		event_streams.ReadPositionAllEnd{},
		false)
	require.NoError(t, err)

	go func() {
		defer wg.Done()

		require.Eventually(t, func() bool {
			_, err := streamReader.ReadOne()
			return err != nil && err.Code() == errors.CanceledErr
		}, time.Second*7, time.Second)
	}()

	time.Sleep(1 * time.Second)
	streamReader.Close()
	// wait for reader to receive stream reader's close
	wg.Wait()
}

func Test_SubscribeToAll_FromStart_ToEmptyDatabase(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	wg := sync.WaitGroup{}
	wg.Add(1)

	streamReader, err := client.SubscribeToStreamAll(context.Background(),
		event_streams.ReadPositionAllEnd{},
		false)
	require.NoError(t, err)

	go func() {
		defer wg.Done()

		require.Eventually(t, func() bool {
			readResult, err := streamReader.ReadOne()

			if event, isEvent := readResult.GetEvent(); isEvent {
				if systemmetadata.IsSystemStream(event.Event.StreamId) {
					t.Fail()
				}
			}

			return err != nil && err.Code() == errors.CanceledErr
		}, time.Second*7, time.Millisecond)
	}()

	streamReader.Close()
	// wait for reader to receive cancellation
	wg.Wait()
}

func Test_SubscribeToAll_FromStart_ReadAllExistingEventsAndKeepListeningForNewOnes(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	wg := sync.WaitGroup{}
	wg.Add(1)

	firstStream := "firstStream"
	secondStream := "secondStream"

	beforeEvents := testCreateEvents(10)
	afterEvents := testCreateEvents(10)

	allUserEvents := append(beforeEvents, afterEvents...)

	_, err := client.AppendToStream(context.Background(),
		firstStream,
		event_streams.WriteStreamRevisionNoStream{},
		beforeEvents)
	require.NoError(t, err)

	streamReader, err := client.SubscribeToStreamAll(context.Background(),
		event_streams.ReadPositionAllStart{},
		false)
	require.NoError(t, err)

	go func() {
		defer wg.Done()

		var resultsRead event_streams.ProposedEventList

		require.Eventually(t, func() bool {
			readResult, err := streamReader.ReadOne()
			require.NoError(t, err)

			if event, isEvent := readResult.GetEvent(); isEvent {
				if !systemmetadata.IsSystemStream(event.Event.StreamId) {
					resultsRead = append(resultsRead, event.ToProposedEvent())
				}
			}

			return reflect.DeepEqual(allUserEvents, resultsRead)
		}, time.Second*10, time.Millisecond)
	}()

	_, err = client.AppendToStream(context.Background(),
		secondStream,
		event_streams.WriteStreamRevisionNoStream{},
		afterEvents)
	require.NoError(t, err)

	// wait for reader to receive events
	wg.Wait()
}

func Test_Test_SubscribeToAll_WithIncorrectCredentials(t *testing.T) {
	client, closeFunc := initializeContainerAndClientWithCredentials(t,
		"wrong_user_name", "wrong_password", nil)
	defer closeFunc()

	_, err := client.SubscribeToStreamAll(context.Background(),
		event_streams.ReadPositionAllStart{},
		false)
	require.Equal(t, errors.UnauthenticatedErr, err.Code())
}
