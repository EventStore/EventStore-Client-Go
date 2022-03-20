package esdb

type SubscriptionEvent struct {
	EventAppeared       *ResolvedEvent
	SubscriptionDropped *SubscriptionDropped
	CheckPointReached   *Position
}

type SubscriptionDropped struct {
	Error error
}
