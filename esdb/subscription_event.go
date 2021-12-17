package esdb

type SubscriptionEvent struct {
	EventAppeared       *ResolvedEvent
	SubscriptionDropped *SubscriptionDropped
	CheckPointReached   *Position
}

type PersistentSubscriptionEvent struct {
	EventAppeared       *EventAppeared
	SubscriptionDropped *SubscriptionDropped
	CheckPointReached   *Position
}
type SubscriptionDropped struct {
	Error error
}

type EventAppeared struct {
	Event      *ResolvedEvent
	RetryCount int
}
