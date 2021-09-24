package subscription

import (
	"github.com/pivonroll/EventStore-Client-Go/messages"
	"github.com/pivonroll/EventStore-Client-Go/position"
)

type SubscriptionEvent struct {
	EventAppeared     *messages.ResolvedEvent
	Dropped           *SubscriptionDropped
	CheckPointReached *position.Position
}

type SubscriptionDropped struct {
	Error error
}
