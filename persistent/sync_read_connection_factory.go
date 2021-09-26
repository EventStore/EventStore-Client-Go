package persistent

//go:generate mockgen -source=sync_read_connection_factory.go -destination=sync_read_connection_factory_mock.go -package=persistent

import (
	"context"

	"github.com/pivonroll/EventStore-Client-Go/protos/persistent"
)

type SyncReadConnectionFactory interface {
	NewSyncReadConnection(client persistent.PersistentSubscriptions_ReadClient,
		subscriptionId string,
		messageAdapter messageAdapter,
		cancel context.CancelFunc,
	) SyncReadConnection
}

type SyncReadConnectionFactoryImpl struct{}

func (factory SyncReadConnectionFactoryImpl) NewSyncReadConnection(
	client persistent.PersistentSubscriptions_ReadClient,
	subscriptionId string,
	messageAdapter messageAdapter,
	cancel context.CancelFunc) SyncReadConnection {

	return newSyncReadConnection(client, subscriptionId, messageAdapter, cancel)
}
