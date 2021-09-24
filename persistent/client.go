package persistent

//go:generate mockgen -source=client.go -destination=client_mock.go -package=persistent

import (
	"context"

	"github.com/pivonroll/EventStore-Client-Go/errors"
)

type Client interface {
	SubscribeToStreamSync(ctx context.Context,
		bufferSize int32,
		groupName string,
		streamName string,
	) (SyncReadConnection, errors.Error)
	CreateStreamSubscription(ctx context.Context, request CreateOrUpdateStreamRequest) errors.Error
	CreateAllSubscription(ctx context.Context, request CreateAllRequest) errors.Error
	UpdateStreamSubscription(ctx context.Context, request CreateOrUpdateStreamRequest) errors.Error
	UpdateAllSubscription(ctx context.Context, request UpdateAllRequest) errors.Error
	DeleteStreamSubscription(ctx context.Context, deleteOptions DeleteRequest) errors.Error
	DeleteAllSubscription(ctx context.Context, groupName string) errors.Error
}
