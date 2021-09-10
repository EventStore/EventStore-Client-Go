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
	messageChannel          chan interface{} // sends items to the user
	maxCacheSize            int
	backOff                 backoff.BackOff
}

type ReaderFunc func() (interface{}, error)

func (reader *ReaderImpl) Start(readerFunc ReaderFunc) <-chan interface{} {
	reader.startOnce.Do(func() {
		reader.messageChannel = make(chan interface{}, reader.maxCacheSize)
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

func (reader *ReaderImpl) loop(readerFunc ReaderFunc) {
	type fetchResult struct {
		fetchedMessage interface{}
		err            error
	}

	var fetchOne chan fetchResult // if non-nil, Fetch is running
	var fetchDelay time.Duration
	var err error
	for {
		// Enable new fetch if we are not fetching or if message cache is not full
		var doFetch <-chan time.Time
		if fetchOne == nil && len(reader.messageChannel) < reader.maxCacheSize {
			doFetch = time.After(fetchDelay) // enable fetch case
		}
		select {
		case <-doFetch: // we are ready to read one message
			fetchOne = make(chan fetchResult, 1)
			go func() {
				fetched, err := readerFunc()
				fetchOne <- fetchResult{fetchedMessage: fetched, err: err}
			}()
		case result := <-fetchOne: // reading of one message is done
			fetchOne = nil // when fetch is done block listening on fetchOne channel

			// set error to the error received from reading last message
			err = result.err
			if result.err != nil { // if error was received initiate backoff mechanism
				if reader.backOff.NextBackOff() == backoff.Stop {
					reader.closeMessageChannel()
					return
				}
				fetchDelay = reader.backOff.NextBackOff()
				break
			}

			reader.backOff.Reset()
			fetchDelay = 0
			reader.messageChannel <- result.fetchedMessage
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
