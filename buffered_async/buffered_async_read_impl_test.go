package buffered_async

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	"github.com/stretchr/testify/require"

	"github.com/cenkalti/backoff/v3"
)

func TestReaderImpl_Start_Reading(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	wg := sync.WaitGroup{}
	wg.Add(1)

	readerHelper := NewMockreaderHelper(ctrl)
	readerHelper.EXPECT().Read().Return("aaaa", nil)
	readerHelper.EXPECT().Read().DoAndReturn(func() (interface{}, error) {
		wg.Wait()
		return "bbb", nil
	})

	reader := NewReaderImpl(1, &backoff.ConstantBackOff{Interval: 2 * time.Minute})
	messageChannel := reader.Start(readerHelper.Read)
	value := <-messageChannel
	require.Equal(t, "aaaa", value)
	wg.Done()
}

func TestReaderImpl_Start_Reading_IsIdempotent(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	wg := sync.WaitGroup{}
	wg.Add(1)

	readerHelper := NewMockreaderHelper(ctrl)
	readerHelper.EXPECT().Read().Return("aaaa", nil)
	readerHelper.EXPECT().Read().DoAndReturn(func() (interface{}, error) {
		wg.Wait()
		return "bbb", nil
	})

	reader := NewReaderImpl(1, &backoff.ConstantBackOff{Interval: 2 * time.Minute})
	reader.Start(readerHelper.Read)
	messageChannel := reader.Start(readerHelper.Read)
	value := <-messageChannel
	require.Equal(t, "aaaa", value)
	wg.Done()
}

func TestReaderImpl_Start_ReadingError_BackoffIsNotStop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	readerWait := sync.WaitGroup{}
	readerWait.Add(2)
	wg := sync.WaitGroup{}
	wg.Add(1)

	readerHelper := NewMockreaderHelper(ctrl)
	errorResult := errors.New("some error")
	firstRead := readerHelper.EXPECT().Read().DoAndReturn(func() (interface{}, error) {
		readerWait.Done()
		return "", errorResult
	})
	secondRead := readerHelper.EXPECT().Read().DoAndReturn(func() (interface{}, error) {
		readerWait.Done()
		return "aaa", nil
	}).After(firstRead)

	readerHelper.EXPECT().Read().DoAndReturn(func() (interface{}, error) {
		wg.Wait()
		return "", nil
	}).After(secondRead).Times(0)

	reader := NewReaderImpl(1, &backoff.ConstantBackOff{Interval: 1 * time.Microsecond})
	messageChannel := reader.Start(readerHelper.Read)
	readerWait.Wait()
	value := <-messageChannel
	require.Equal(t, "aaa", value)
	wg.Done()
}

func TestReaderImpl_Start_ReadingError_BackoffIsStop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	readerWait := sync.WaitGroup{}
	readerWait.Add(1)
	wg := sync.WaitGroup{}
	wg.Add(1)

	readerHelper := NewMockreaderHelper(ctrl)
	errorResult := errors.New("some error")
	firstRead := readerHelper.EXPECT().Read().DoAndReturn(func() (interface{}, error) {
		readerWait.Done()
		return "", errorResult
	})
	readerHelper.EXPECT().Read().DoAndReturn(func() (interface{}, error) {
		return "aaa", nil
	}).After(firstRead).Times(0)

	reader := NewReaderImpl(1, &backoff.StopBackOff{})
	messageChannel := reader.Start(readerHelper.Read)
	readerWait.Wait()
	value := <-messageChannel
	require.Equal(t, nil, value)
}

func TestReaderImpl_StartAndStop_ReadingError_BackoffIsStop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	readerWait := sync.WaitGroup{}
	readerWait.Add(1)

	readerHelper := NewMockreaderHelper(ctrl)
	errorResult := errors.New("some error")
	firstRead := readerHelper.EXPECT().Read().DoAndReturn(func() (interface{}, error) {
		readerWait.Done()
		return "", errorResult
	})
	readerHelper.EXPECT().Read().DoAndReturn(func() (interface{}, error) {
		return "aaa", nil
	}).After(firstRead).Times(0)

	reader := NewReaderImpl(1, &backoff.StopBackOff{})
	reader.Start(readerHelper.Read)
	readerWait.Wait()
	err := reader.Stop()
	require.NoError(t, err)
}

func TestReaderImpl_StartAndStop_ReadingError_BackoffIsNotStop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	readerWait := sync.WaitGroup{}
	readerWait.Add(1)
	wg := sync.WaitGroup{}
	wg.Add(1)

	readerHelper := NewMockreaderHelper(ctrl)
	errorResult := errors.New("some error")
	firstRead := readerHelper.EXPECT().Read().DoAndReturn(func() (interface{}, error) {
		readerWait.Done()
		return "", errorResult
	})
	readerHelper.EXPECT().Read().DoAndReturn(func() (interface{}, error) {
		wg.Wait()
		return "aaa", nil
	}).After(firstRead).Times(0)

	reader := NewReaderImpl(1, &backoff.ConstantBackOff{Interval: 2 * time.Microsecond})
	reader.Start(readerHelper.Read)
	readerWait.Wait()
	err := reader.Stop()
	require.EqualError(t, err, errorResult.Error())
	wg.Done()
}

func TestReaderImpl_StartAndStop_WithReadingOneMessage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	readWait := sync.WaitGroup{}
	readWait.Add(1)

	readerHelper := NewMockreaderHelper(ctrl)
	readerHelper.EXPECT().Read().DoAndReturn(func() (interface{}, error) {
		readWait.Done()
		return "aaa", nil
	})

	reader := NewReaderImpl(1, &backoff.ConstantBackOff{Interval: 2 * time.Minute})
	reader.Start(readerHelper.Read)
	readWait.Wait()
	err := reader.Stop()
	require.NoError(t, err)
}

func TestReaderImpl_StartAndStop_WithoutReadingAnyMessage(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	readWait := sync.WaitGroup{}
	readWait.Add(1)

	readerHelper := NewMockreaderHelper(ctrl)
	readerHelper.EXPECT().Read().DoAndReturn(func() (interface{}, error) {
		readWait.Wait()
		return "aaa", nil
	}).Times(0)

	reader := NewReaderImpl(1, &backoff.ConstantBackOff{Interval: 2 * time.Minute})
	reader.Start(readerHelper.Read)
	err := reader.Stop()
	require.NoError(t, err)
	readWait.Done()
}

func TestReaderImpl_Stop_After_Start_WithoutReadingAnyMessage_IsIdempotent(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	readWait := sync.WaitGroup{}
	readWait.Add(1)

	readerHelper := NewMockreaderHelper(ctrl)
	readerHelper.EXPECT().Read().DoAndReturn(func() (interface{}, error) {
		readWait.Wait()
		return "aaa", nil
	}).Times(0)

	reader := NewReaderImpl(1, &backoff.ConstantBackOff{Interval: 2 * time.Minute})
	reader.Start(readerHelper.Read)
	err := reader.Stop()
	require.NoError(t, err)
	err = reader.Stop()
	require.NoError(t, err)
	readWait.Done()
}

func TestReaderImpl_Stop_BeforeStarting(t *testing.T) {
	reader := NewReaderImpl(10, &backoff.ConstantBackOff{Interval: 2 * time.Minute})
	err := reader.Stop()
	require.NoError(t, err)
}

func TestReaderImpl_Stop_BeforeStarting_IsIdempotent(t *testing.T) {
	reader := NewReaderImpl(10, &backoff.ConstantBackOff{Interval: 2 * time.Minute})
	err := reader.Stop()
	require.NoError(t, err)
	err = reader.Stop()
	require.NoError(t, err)
}
