package buffered_async

import (
	"sync"
	"time"

	"github.com/pivonroll/EventStore-Client-Go/errors"
)

type ReaderImpl struct {
	startOnce               sync.Once
	stopOnce                sync.Once
	closeMessageChannelOnce sync.Once
	stopRequestChannel      chan chan error
	messageChannel          chan FetchResult // sends items to the user
	maxCacheSize            int
	fetchedItems            []FetchResult
}

type ReaderFunc func() (interface{}, errors.Error)

func (reader *ReaderImpl) Start(readerFunc ReaderFunc) <-chan FetchResult {
	reader.startOnce.Do(func() {
		reader.messageChannel = make(chan FetchResult, reader.maxCacheSize)
		reader.closeMessageChannelOnce = sync.Once{}
		go reader.loop(readerFunc)
		reader.stopOnce = sync.Once{}
	})

	return reader.messageChannel
}

func (reader *ReaderImpl) Stop() {
	reader.stopOnce.Do(func() {
		if reader.stopRequestChannel != nil {
			stopResponseChannel := make(chan error)
			reader.stopRequestChannel <- stopResponseChannel
			<-stopResponseChannel
		}
		reader.startOnce = sync.Once{}
	})
}

type FetchResult struct {
	FetchedMessage interface{}
	Err            errors.Error
}

func (reader *ReaderImpl) loop(readerFunc ReaderFunc) {
	var fetchOne chan FetchResult // if non-nil, Fetch is running
	reader.stopRequestChannel = make(chan chan error)

	for {
		// Enable new fetch if we are not fetching or if message buffer is not full
		var doFetch <-chan time.Time
		if fetchOne == nil &&
			len(reader.messageChannel) < reader.maxCacheSize {
			doFetch = time.After(0) // enable fetch case
		}

		select {
		case <-doFetch: // we are ready to read one message
			fetchOne = make(chan FetchResult, 1)
			go func() { // we do not want to block while we are waiting to fetch new message
				fetched, err := readerFunc()
				fetchOne <- FetchResult{FetchedMessage: fetched, Err: err}
			}()
		case result := <-fetchOne: // reading of one message is done
			reader.messageChannel <- result

			// if error was received stop reading by letting fetchOne be non-nil
			if result.Err != nil {
				reader.closeMessageChannel()
			} else {
				fetchOne = nil // when fetch is done, without error received, allow to fetch a new one
			}
		case stopResponseChannel := <-reader.stopRequestChannel: // if Stop is initiated
			// if there is a message read but not sent to message channel send it
			if fetchOne != nil && reader.messageChannel != nil {
				reader.messageChannel <- <-fetchOne
			}
			reader.closeMessageChannel()
			stopResponseChannel <- nil
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

func NewReaderImpl(maxCacheSize int) *ReaderImpl {
	return &ReaderImpl{
		startOnce:               sync.Once{},
		stopOnce:                sync.Once{},
		closeMessageChannelOnce: sync.Once{},
		stopRequestChannel:      nil,
		maxCacheSize:            maxCacheSize,
		messageChannel:          nil,
	}
}
