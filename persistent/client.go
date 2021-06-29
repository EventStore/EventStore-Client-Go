package persistent

import (
	"context"
)

type Client interface {
	SubscribeToStreamSync(ctx context.Context,
		bufferSize int32,
		groupName string,
		streamName []byte,
	) (SyncReadConnection, error)
	CreateStreamSubscription(ctx context.Context, streamConfig SubscriptionStreamConfig) error
	CreateAllSubscription(ctx context.Context, allOptions SubscriptionAllOptionConfig) error
	UpdateStreamSubscription(ctx context.Context, streamConfig SubscriptionStreamConfig) error
	UpdateAllSubscription(ctx context.Context, allOptions SubscriptionUpdateAllOptionConfig) error
	DeleteStreamSubscription(ctx context.Context, deleteOptions DeleteOptions) error
	DeleteAllSubscription(ctx context.Context, groupName string) error
}
