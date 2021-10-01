package event_streams_integration_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pivonroll/EventStore-Client-Go/errors"

	"github.com/stretchr/testify/require"

	"github.com/pivonroll/EventStore-Client-Go/event_streams"
)

func Test_SubscribeToAll_FromStart_ReturnsSubscriptionDroppedWhenCancelled(t *testing.T) {
	t.Skip("When we subscribe to all from start, subscription cannot be cancelled")
	client, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	wg := sync.WaitGroup{}
	wg.Add(1)

	ctx, cancelFunc := context.WithCancel(context.Background())
	streamReader, err := client.SubscribeToAll(ctx,
		event_streams.SubscribeRequestOptionsAllStartPosition{},
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

func Test_SubscribeToAll_FromEnd_ReturnsSubscriptionDroppedWhenCancelled(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	wg := sync.WaitGroup{}
	wg.Add(1)

	ctx, cancelFunc := context.WithCancel(context.Background())
	streamReader, err := client.SubscribeToAll(ctx,
		event_streams.SubscribeRequestOptionsAllEndPosition{},
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
	t.Skip("When we subscribe to all from start, subscription cannot be cancelled")
	client, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	wg := sync.WaitGroup{}
	wg.Add(1)

	streamReader, err := client.SubscribeToAll(context.Background(),
		event_streams.SubscribeRequestOptionsAllStartPosition{},
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
	// wait for reader to receive cancellation
	wg.Wait()
}

func Test_SubscribeToAll_FromEnd_ReturnsSubscriptionDroppedWhenReaderClosed(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	wg := sync.WaitGroup{}
	wg.Add(1)

	streamReader, err := client.SubscribeToAll(context.Background(),
		event_streams.SubscribeRequestOptionsAllEndPosition{},
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
	// wait for reader to receive cancellation
	wg.Wait()
}
