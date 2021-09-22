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
		request CreateOrUpdateStreamRequest) errors.Error
	CreateAllSubscription(ctx context.Context,
		handle connection.ConnectionHandle,
		request CreateAllRequest) errors.Error
	UpdateStreamSubscription(ctx context.Context,
		handle connection.ConnectionHandle,
		request CreateOrUpdateStreamRequest) errors.Error
	UpdateAllSubscription(ctx context.Context,
		handle connection.ConnectionHandle,
		request UpdateAllRequest) errors.Error
	DeleteStreamSubscription(ctx context.Context,
		handle connection.ConnectionHandle,
		deleteOptions DeleteRequest) errors.Error
	DeleteAllSubscription(ctx context.Context,
		handle connection.ConnectionHandle,
		groupName string) errors.Error
}
