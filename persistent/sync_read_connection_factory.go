package persistent

type SyncReadConnectionFactory interface {
	NewSyncReadConnection(client protoClient,
		subscriptionId string,
		messageAdapter messageAdapter,
	) SyncReadConnection
}

type SyncReadConnectionFactoryImpl struct{}

func (factory SyncReadConnectionFactoryImpl) NewSyncReadConnection(
	client protoClient,
	subscriptionId string,
	messageAdapter messageAdapter) SyncReadConnection {
	return &syncConnectionImpl{
		client:         client,
		subscriptionId: subscriptionId,
		messageAdapter: messageAdapter,
	}
}
