package esdb

import "time"

// PersistentStreamSubscriptionOptions options for most of the persistent subscription requests.
type PersistentStreamSubscriptionOptions struct {
	// Persistent subscription's request.
	Settings *PersistentSubscriptionSettings
	// Starting position of the subscription.
	StartFrom StreamPosition
	// Asks for authenticated request.
	Authenticated *Credentials
	// A length of time to use for gRPC deadlines.
	Deadline *time.Duration
}

func (o *PersistentStreamSubscriptionOptions) kind() operationKind {
	return regularOperation
}

func (o *PersistentStreamSubscriptionOptions) credentials() *Credentials {
	return o.Authenticated
}

func (o *PersistentStreamSubscriptionOptions) deadline() *time.Duration {
	return o.Deadline
}

func (o *PersistentStreamSubscriptionOptions) setDefaults() {
	if o.StartFrom == nil {
		o.StartFrom = End{}
	}
}

func (o *PersistentStreamSubscriptionOptions) requiresLeader() bool {
	return true
}

// PersistentAllSubscriptionOptions options for most of the persistent subscription requests.
type PersistentAllSubscriptionOptions struct {
	// Persistent subscription's request.
	Settings *PersistentSubscriptionSettings
	// Starting position of the subscription.
	StartFrom AllPosition
	// Max search window.
	MaxSearchWindow int
	// Applies a server-side filter to determine if an event of the subscription should be yielded.
	Filter *SubscriptionFilter
	// Asks for authenticated request.
	Authenticated *Credentials
	// A length of time to use for gRPC deadlines.
	Deadline *time.Duration
}

func (o *PersistentAllSubscriptionOptions) kind() operationKind {
	return regularOperation
}

func (o *PersistentAllSubscriptionOptions) credentials() *Credentials {
	return o.Authenticated
}

func (o *PersistentAllSubscriptionOptions) deadline() *time.Duration {
	return o.Deadline
}

func (o *PersistentAllSubscriptionOptions) requiresLeader() bool {
	return true
}

func (o *PersistentAllSubscriptionOptions) setDefaults() {
	if o.StartFrom == nil {
		o.StartFrom = End{}
	}

	if o.Filter != nil {
		if o.MaxSearchWindow == 0 {
			o.MaxSearchWindow = 32
		}
	}
}

// SubscribeToPersistentSubscriptionOptions options of the subscribe to persistent subscription request.
type SubscribeToPersistentSubscriptionOptions struct {
	// Buffer size.
	BufferSize uint32
	// Asks for authenticated request.
	Authenticated *Credentials
	// A length of time to use for gRPC deadlines.
	Deadline *time.Duration
}

func (o *SubscribeToPersistentSubscriptionOptions) kind() operationKind {
	return streamingOperation
}

func (o *SubscribeToPersistentSubscriptionOptions) credentials() *Credentials {
	return o.Authenticated
}

func (o *SubscribeToPersistentSubscriptionOptions) deadline() *time.Duration {
	return o.Deadline
}

func (o *SubscribeToPersistentSubscriptionOptions) requiresLeader() bool {
	return true
}

func (o *SubscribeToPersistentSubscriptionOptions) setDefaults() {
	if o.BufferSize == 0 {
		o.BufferSize = 10
	}
}

// DeletePersistentSubscriptionOptions options of the delete persistent subscription's request.
type DeletePersistentSubscriptionOptions struct {
	// Asks for authenticated request.
	Authenticated *Credentials
	// A length of time to use for gRPC deadlines.
	Deadline *time.Duration
}

func (d *DeletePersistentSubscriptionOptions) kind() operationKind {
	return regularOperation
}

func (d *DeletePersistentSubscriptionOptions) credentials() *Credentials {
	return d.Authenticated
}

func (d *DeletePersistentSubscriptionOptions) deadline() *time.Duration {
	return d.Deadline
}

func (d *DeletePersistentSubscriptionOptions) requiresLeader() bool {
	return true
}

// ReplayParkedMessagesOptions options of the replay parked messages request.
type ReplayParkedMessagesOptions struct {
	// Asks for authenticated request.
	Authenticated *Credentials
	// Replays the parked messages until the event revision within the parked messages stream is reached.
	StopAt int
	// A length of time to use for gRPC deadlines.
	Deadline *time.Duration
}

func (r *ReplayParkedMessagesOptions) kind() operationKind {
	return regularOperation
}

func (r *ReplayParkedMessagesOptions) credentials() *Credentials {
	return r.Authenticated
}

func (r *ReplayParkedMessagesOptions) deadline() *time.Duration {
	return r.Deadline
}

func (r *ReplayParkedMessagesOptions) requiresLeader() bool {
	return true
}

// ListPersistentSubscriptionsOptions options of the list persistent subscription request.
type ListPersistentSubscriptionsOptions struct {
	// Asks for authenticated request.
	Authenticated *Credentials
	// A length of time to use for gRPC deadlines.
	Deadline *time.Duration
}

func (l *ListPersistentSubscriptionsOptions) kind() operationKind {
	return regularOperation
}

func (l *ListPersistentSubscriptionsOptions) credentials() *Credentials {
	return l.Authenticated
}

func (l *ListPersistentSubscriptionsOptions) deadline() *time.Duration {
	return l.Deadline
}

func (l *ListPersistentSubscriptionsOptions) requiresLeader() bool {
	return true
}

// GetPersistentSubscriptionOptions options of the get persistent subscription info request.
type GetPersistentSubscriptionOptions struct {
	// Asks for authenticated request.
	Authenticated *Credentials
	// A length of time to use for gRPC deadlines.
	Deadline *time.Duration
}

func (g *GetPersistentSubscriptionOptions) kind() operationKind {
	return regularOperation
}

func (g *GetPersistentSubscriptionOptions) credentials() *Credentials {
	return g.Authenticated
}

func (g *GetPersistentSubscriptionOptions) deadline() *time.Duration {
	return g.Deadline
}

func (g *GetPersistentSubscriptionOptions) requiresLeader() bool {
	return true
}

// RestartPersistentSubscriptionSubsystemOptions options of the restart persistent subscription subsystem request.
type RestartPersistentSubscriptionSubsystemOptions struct {
	// Asks for authenticated request.
	Authenticated *Credentials
	// A length of time to use for gRPC deadlines.
	Deadline *time.Duration
}

func (g *RestartPersistentSubscriptionSubsystemOptions) kind() operationKind {
	return regularOperation
}

func (g *RestartPersistentSubscriptionSubsystemOptions) credentials() *Credentials {
	return g.Authenticated
}

func (g *RestartPersistentSubscriptionSubsystemOptions) deadline() *time.Duration {
	return g.Deadline
}

func (g *RestartPersistentSubscriptionSubsystemOptions) requiresLeader() bool {
	return true
}
