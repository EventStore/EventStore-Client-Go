package persistent

import (
	"context"

	"github.com/EventStore/EventStore-Client-Go/messages"
	"github.com/gofrs/uuid"
)

type Client interface {
	SubscribeToStreamAsync(ctx context.Context,
		bufferSize int32,
		groupName string,
		streamName []byte,
		eventAppeared EventAppearedHandler,
		subscriptionDropped SubscriptionDroppedHandler,
	) (Connection, error)
	SubscribeToAllAsync(ctx context.Context) (Connection, error)
	CreateStreamSubscription(ctx context.Context, streamConfig SubscriptionStreamConfig) error
	CreateAllSubscription(ctx context.Context, allOptions SubscriptionAllOptionConfig) error
	UpdateStreamSubscription(ctx context.Context, streamConfig SubscriptionStreamConfig) error
	UpdateAllSubscription(ctx context.Context, allOptions SubscriptionUpdateAllOptionConfig) error
	DeleteStreamSubscription(ctx context.Context, deleteOptions DeleteOptions) error
	DeleteAllSubscription(ctx context.Context, groupName string) error
}

type Nack_Action int32

const (
	Nack_Unknown Nack_Action = 0
	Nack_Park    Nack_Action = 1
	Nack_Retry   Nack_Action = 2
	Nack_Skip    Nack_Action = 3
	Nack_Stop    Nack_Action = 4
)

type Connection interface {
	Read() (*messages.RecordedEvent, error) // blocking call
	Ack(messageIds ...uuid.UUID) error      // max 2000 messages can be acknowledged
	Nack(reason string, action Nack_Action, messageIds ...uuid.UUID) error
}
