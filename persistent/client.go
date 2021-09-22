package persistent

//go:generate mockgen -source=client.go -destination=client_mock.go -package=persistent

import (
	"context"

	"github.com/EventStore/EventStore-Client-Go/connection"
	"github.com/EventStore/EventStore-Client-Go/errors"
)

type Client interface {
	SubscribeToStreamSync(ctx context.Context,
		handle connection.ConnectionHandle,
		bufferSize int32,
		groupName string,
		streamName string,
	) (SyncReadConnection, errors.Error)
	CreateStreamSubscription(ctx context.Context,
		handle connection.ConnectionHandle,
		streamConfig SubscriptionStreamConfig) errors.Error
	CreateAllSubscription(ctx context.Context,
		handle connection.ConnectionHandle,
		allOptions SubscriptionAllOptionConfig) errors.Error
	UpdateStreamSubscription(ctx context.Context,
		handle connection.ConnectionHandle,
		streamConfig SubscriptionStreamConfig) errors.Error
	UpdateAllSubscription(ctx context.Context,
		handle connection.ConnectionHandle,
		allOptions SubscriptionUpdateAllOptionConfig) errors.Error
	DeleteStreamSubscription(ctx context.Context,
		handle connection.ConnectionHandle,
		deleteOptions DeleteOptions) errors.Error
	DeleteAllSubscription(ctx context.Context,
		handle connection.ConnectionHandle,
		groupName string) errors.Error
}
