package persistent

//go:generate mockgen -source=event_reader_factory.go -destination=event_reader_factory_mock.go -package=persistent

import (
	"context"

	"github.com/pivonroll/EventStore-Client-Go/protos/persistent"
)

type eventReaderFactory interface {
	Create(client persistent.PersistentSubscriptions_ReadClient,
		subscriptionId string,
		messageAdapter messageAdapter,
		cancel context.CancelFunc,
	) EventReader
}

type eventReaderFactoryImpl struct{}

func (factory eventReaderFactoryImpl) Create(
	client persistent.PersistentSubscriptions_ReadClient,
	subscriptionId string,
	messageAdapter messageAdapter,
	cancel context.CancelFunc) EventReader {

	return newEventReader(client, subscriptionId, messageAdapter, cancel)
}