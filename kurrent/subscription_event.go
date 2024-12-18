package kurrent

// SubscriptionEvent used to handle catch-up subscription notifications raised throughout its lifecycle.
type SubscriptionEvent struct {
	// When KurrentDB sends an event to the subscription.
	EventAppeared *ResolvedEvent
	// When the subscription is dropped.
	SubscriptionDropped *SubscriptionDropped
	// When a checkpoint was created.
	CheckPointReached *Position
	// When an event is caught up
	CaughtUp *Subscription
	// When an event has fallen behind
	FellBehind *Subscription
}

// PersistentSubscriptionEvent used to handle persistent subscription notifications raised throughout its lifecycle.
type PersistentSubscriptionEvent struct {
	// When KurrentDB sends an event to the subscription.
	EventAppeared *EventAppeared
	// When the subscription is dropped.
	SubscriptionDropped *SubscriptionDropped
	// When a checkpoint was created.
	CheckPointReached *Position
}

// SubscriptionDropped when a subscription is dropped.
type SubscriptionDropped struct {
	// Error that caused the drop.
	Error error
}

// EventAppeared when KurrentDB sends an event to a persistent subscription.
type EventAppeared struct {
	// Event sent by KurrentDB.
	Event *ResolvedEvent
	// How many times that event was sent to the persistent subscription.
	RetryCount int
}
