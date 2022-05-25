package esdb

// SubscriptionEvent used to handle catch-up subscription notifications raised throughout its lifecycle.
type SubscriptionEvent struct {
	// When EventStoreDB sends an event to the subscription.
	EventAppeared *ResolvedEvent
	// When the subscription is dropped.
	SubscriptionDropped *SubscriptionDropped
	// When a checkpoint was created.
	CheckPointReached *Position
}

// PersistentSubscriptionEvent used to handle persistent subscription notifications raised throughout its lifecycle.
type PersistentSubscriptionEvent struct {
	// When EventStoreDB sends an event to the subscription.
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

// EventAppeared when EventStoreDB sends an event to a persistent subscription.
type EventAppeared struct {
	// Event sent by EventStoreDB.
	Event *ResolvedEvent
	// How many times that event was sent to the persistent subscription.
	RetryCount int
}
