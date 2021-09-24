package buffered_async

import (
	"sync"
	"time"

	"github.com/cenkalti/backoff/v3"
)

type ReaderImpl struct {
	startOnce               sync.Once
	stopOnce                sync.Once
	closeMessageChannelOnce sync.Once
	stopRequestChannel      chan chan error
	messageChannel          chan FetchResult // sends items to the user
	maxCacheSize            int
	backOff                 backoff.BackOff
}

type ReaderFunc func() (interface{}, error)

func (reader *ReaderImpl) Start(readerFunc ReaderFunc) <-chan FetchResult {
	reader.startOnce.Do(func() {
		reader.messageChannel = make(chan FetchResult, reader.maxCacheSize)
		reader.closeMessageChannelOnce = sync.Once{}
		go reader.loop(readerFunc)
		reader.stopOnce = sync.Once{}
	})

	return reader.messageChannel
}

func (reader *ReaderImpl) Stop() error {
	var err error
	reader.stopOnce.Do(func() {
		if reader.messageChannel != nil {
			stopResponseChannel := make(chan error)
			reader.stopRequestChannel <- stopResponseChannel
			err = <-stopResponseChannel
			reader.startOnce = sync.Once{}
		}
	})

	return err
}

func (reader *ReaderImpl) isBufferFull() bool {
	return len(reader.messageChannel) >= reader.maxCacheSize
}

type FetchResult struct {
	FetchedMessage interface{}
	Err            error
}

func (reader *ReaderImpl) loop(readerFunc ReaderFunc) {
	var fetchOne chan FetchResult // if non-nil, Fetch is running
	var fetchDelay time.Duration
	var err error
	for {
		// Enable new fetch if we are not fetching or if message buffer is not full
		var doFetch <-chan time.Time
		if fetchOne == nil && !reader.isBufferFull() {
			doFetch = time.After(fetchDelay) // enable fetch case
		}
		select {
		case <-doFetch: // we are ready to read one message
			fetchOne = make(chan FetchResult, 1)
			go func() {
				fetched, err := readerFunc()
				fetchOne <- FetchResult{FetchedMessage: fetched, Err: err}
			}()
		case result := <-fetchOne: // reading of one message is done
			fetchOne = nil // when fetch is done block listening on fetchOne channel

			// set error to the error received from reading last message
			err = result.Err
			if result.Err != nil { // if error was received initiate backoff mechanism
				if reader.backOff.NextBackOff() == backoff.Stop {
					reader.messageChannel <- result
					reader.closeMessageChannel()
					return
				}
				fetchDelay = reader.backOff.NextBackOff()
				break
			}

			reader.backOff.Reset()
			fetchDelay = 0
			reader.messageChannel <- result
		case stopResponseChannel := <-reader.stopRequestChannel: // if Stop is initiated
			// return error received from reading last message
			stopResponseChannel <- err
			reader.closeMessageChannel()
			return
		}
	}
}

func (reader *ReaderImpl) closeMessageChannel() {
	reader.closeMessageChannelOnce.Do(func() {
		if reader.messageChannel != nil {
			close(reader.messageChannel)
			reader.messageChannel = nil
		}
	})
}

func NewReaderImpl(maxCacheSize int, backOff backoff.BackOff) *ReaderImpl {
	return &ReaderImpl{
		startOnce:               sync.Once{},
		stopOnce:                sync.Once{},
		closeMessageChannelOnce: sync.Once{},
		stopRequestChannel:      make(chan chan error),
		maxCacheSize:            maxCacheSize,
		backOff:                 backOff,
		messageChannel:          nil,
	}
}
