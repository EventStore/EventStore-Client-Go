package persistent

import (
	"errors"
	"fmt"

	"github.com/EventStore/EventStore-Client-Go/messages"
	"github.com/EventStore/EventStore-Client-Go/protos/persistent"
	"github.com/davecgh/go-spew/spew"
)

type asyncConnectionImpl struct {
	quitChannel      chan struct{}
	listeningChannel chan messages.RecordedEvent
	handler          EventAppearedHandler
	readConnection   SyncReadConnection
}

func newAsyncConnectionImpl(
	client persistent.PersistentSubscriptions_ReadClient,
	subscriptionId string,
) AsyncReadConnection {
	return &asyncConnectionImpl{
		readConnection: newSyncReadConnection(client, subscriptionId, messageAdapterImpl{}),
	}
}

func (asyncConnection *asyncConnectionImpl) RegisterHandler(handler EventAppearedHandler) {
	asyncConnection.handler = handler
}

func (asyncConnection *asyncConnectionImpl) Start(retryMessageHandlingCount uint8) error {
	if retryMessageHandlingCount == 0 {
		return errors.New("retry count must be greater than 1")
	}

	asyncConnection.quitChannel = make(chan struct{})
	asyncConnection.listeningChannel = make(chan messages.RecordedEvent)

	// async message read
	asyncConnection.startAsyncRead()

listeningLoop:
	for {
		select {
		case <-asyncConnection.quitChannel:
			break listeningLoop
		case message := <-asyncConnection.listeningChannel:
			// retry message processing
			asyncConnection.retryMessageHandling(retryMessageHandlingCount, message)
		}
	}

	return nil
}

func (asyncConnection *asyncConnectionImpl) Stop() {
	if asyncConnection.quitChannel != nil {
		asyncConnection.quitChannel <- struct{}{}
	}
}

func (asyncConnection *asyncConnectionImpl) startAsyncRead() {
	// async message read
	go func() {
		for {
			message, err := asyncConnection.readConnection.Read()
			if err != nil {
				// stop processing
				asyncConnection.quitChannel <- struct{}{}
			}

			// if no message was read on the subscription stream then skip
			if message == nil {
				continue
			}

			asyncConnection.listeningChannel <- *message
		}
	}()
}

func (asyncConnection *asyncConnectionImpl) retryMessageHandling(retryMessageHandlingCount uint8,
	message messages.RecordedEvent) {
	var i uint8 = 0

retryHandlerLoop:
	for ; i < retryMessageHandlingCount; i++ {
		err := asyncConnection.handler(message)
		if err == nil {
			break retryHandlerLoop
		}
	}

	// if retry mechanism failed send Nack
	if i >= retryMessageHandlingCount {
		reason := fmt.Sprintf("Retry mechanism failed. Failed to process message %v",
			spew.Sdump(message))
		err := asyncConnection.readConnection.Nack(reason, Nack_Retry, message.EventID)
		if err != nil {
			fmt.Println(err)
		}
	}
}
