package persistent

//go:generate mockgen -source=client.go -destination=client_mock.go -package=persistent

import (
	"context"

	"github.com/EventStore/EventStore-Client-Go/connection"
)

type Client interface {
	SubscribeToStreamSync(ctx context.Context,
		handle connection.ConnectionHandle,
		bufferSize int32,
		groupName string,
		streamName []byte,
	) (SyncReadConnection, error)
	CreateStreamSubscription(ctx context.Context, handle connection.ConnectionHandle, streamConfig SubscriptionStreamConfig) error
	CreateAllSubscription(ctx context.Context, handle connection.ConnectionHandle, allOptions SubscriptionAllOptionConfig) error
	UpdateStreamSubscription(ctx context.Context, handle connection.ConnectionHandle, streamConfig SubscriptionStreamConfig) error
	UpdateAllSubscription(ctx context.Context, handle connection.ConnectionHandle, allOptions SubscriptionUpdateAllOptionConfig) error
	DeleteStreamSubscription(ctx context.Context, handle connection.ConnectionHandle, deleteOptions DeleteOptions) error
	DeleteAllSubscription(ctx context.Context, handle connection.ConnectionHandle, groupName string) error
}
