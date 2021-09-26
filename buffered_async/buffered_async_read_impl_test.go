package buffered_async

import (
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pivonroll/EventStore-Client-Go/errors"

	"github.com/stretchr/testify/require"
)

func TestReaderImpl_Start_Reading(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	readerStartWait := sync.WaitGroup{}
	readerStartWait.Add(1)

	readerHelper := NewMockreaderHelper(ctrl)
	readerHelper.EXPECT().Read().Return("aaaa", nil)
	readerHelper.EXPECT().Read().DoAndReturn(func() (interface{}, error) {
		readerStartWait.Wait()
		return "bbb", nil
	})

	reader := NewReaderImpl(1)
	messageChannel := reader.Start(readerHelper.Read)
	value := <-messageChannel
	require.Equal(t, FetchResult{FetchedMessage: "aaaa", Err: nil}, value)
	readerStartWait.Done()
}

func TestReaderImpl_Start_Reading_WaitsForAvailableSpotInBuffer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	secondReaderWait := sync.WaitGroup{}
	secondReaderWait.Add(1)
	thirdReaderWait := sync.WaitGroup{}
	thirdReaderWait.Add(1)

	readerHelper := NewMockreaderHelper(ctrl)

	gomock.InOrder(
		readerHelper.EXPECT().Read().Return("aaaa", nil),
		readerHelper.EXPECT().Read().DoAndReturn(func() (interface{}, error) {
			secondReaderWait.Wait()
			return "bbb", nil
		}),
		readerHelper.EXPECT().Read().DoAndReturn(func() (interface{}, error) {
			thirdReaderWait.Wait()
			return "bbb", nil
		}).MaxTimes(1),
	)

	reader := NewReaderImpl(1)
	messageChannel := reader.Start(readerHelper.Read)
	value := <-messageChannel
	require.Equal(t, FetchResult{FetchedMessage: "aaaa", Err: nil}, value)
	require.Len(t, messageChannel, 0)
	secondReaderWait.Done()
	value = <-messageChannel
	require.Equal(t, FetchResult{FetchedMessage: "bbb", Err: nil}, value)
	thirdReaderWait.Done()
}

func TestReaderImpl_Start_Reading_IsIdempotent(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	wg := sync.WaitGroup{}
	wg.Add(1)

	readerHelper := NewMockreaderHelper(ctrl)
	gomock.InOrder(
		readerHelper.EXPECT().Read().Return("aaaa", nil),
		readerHelper.EXPECT().Read().DoAndReturn(func() (interface{}, error) {
			wg.Wait()
			return "bbb", nil
		}).MaxTimes(1),
	)
	reader := NewReaderImpl(1)
	reader.Start(readerHelper.Read)
	messageChannel := reader.Start(readerHelper.Read)
	value := <-messageChannel
	require.Equal(t, FetchResult{FetchedMessage: "aaaa", Err: nil}, value)
	wg.Done()
}

func TestReaderImpl_Start_ReadingError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	readerWait := sync.WaitGroup{}
	readerWait.Add(1)
	wg := sync.WaitGroup{}
	wg.Add(1)

	readerHelper := NewMockreaderHelper(ctrl)
	errorResult := errors.NewErrorCode("some error")

	gomock.InOrder(
		readerHelper.EXPECT().Read().DoAndReturn(func() (interface{}, error) {
			readerWait.Done()
			return "", errorResult
		}),
		readerHelper.EXPECT().Read().DoAndReturn(func() (interface{}, error) {
			return "aaa", nil
		}).Times(0),
	)

	reader := NewReaderImpl(1)
	messageChannel := reader.Start(readerHelper.Read)
	readerWait.Wait()
	value := <-messageChannel
	require.Equal(t, FetchResult{FetchedMessage: "", Err: errorResult}, value)
}

func TestReaderImpl_StartAndStop_ReadingError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	readerWait := sync.WaitGroup{}
	readerWait.Add(1)

	readerHelper := NewMockreaderHelper(ctrl)
	errorResult := errors.NewErrorCode("some error")

	gomock.InOrder(
		readerHelper.EXPECT().Read().DoAndReturn(func() (interface{}, error) {
			readerWait.Done()
			return "", errorResult
		}),
		readerHelper.EXPECT().Read().DoAndReturn(func() (interface{}, error) {
			return "aaa", nil
		}).Times(0),
	)

	reader := NewReaderImpl(1)
	readChannel := reader.Start(readerHelper.Read)
	readerWait.Wait()
	reader.Stop()

	value, ok := <-readChannel
	require.Equal(t, FetchResult{
		FetchedMessage: "",
		Err:            errorResult,
	}, value)
	require.True(t, ok)

	value, ok = <-readChannel
	require.Equal(t, FetchResult{}, value)
	require.False(t, ok)
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

	reader := NewReaderImpl(1)
	reader.Start(readerHelper.Read)
	readWait.Wait()
	reader.Stop()
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
	}).MaxTimes(1)

	reader := NewReaderImpl(1)
	reader.Start(readerHelper.Read)
	reader.Stop()
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
	}).MaxTimes(1)

	reader := NewReaderImpl(1)
	reader.Start(readerHelper.Read)
	reader.Stop()
	reader.Stop()
	readWait.Done()
}

func TestReaderImpl_Stop_BeforeStarting(t *testing.T) {
	reader := NewReaderImpl(10)
	reader.Stop()
}

func TestReaderImpl_Stop_BeforeStarting_IsIdempotent(t *testing.T) {
	reader := NewReaderImpl(10)
	reader.Stop()
	reader.Stop()
}
