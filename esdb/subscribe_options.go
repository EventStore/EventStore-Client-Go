package esdb

import (
	"time"
)

// SubscribeToStreamOptions options of the subscribe to stream request.
type SubscribeToStreamOptions struct {
	// Starting position of the subscribe request.
	From StreamPosition
	// Whether the read request should resolve linkTo events to their linked events.
	ResolveLinkTos bool
	// Asks for authenticated request.
	Authenticated *Credentials
	// A length of time to use for gRPC deadlines.
	Deadline *time.Duration
}

func (o *SubscribeToStreamOptions) kind() operationKind {
	return streamingOperation
}

func (o *SubscribeToStreamOptions) credentials() *Credentials {
	return o.Authenticated
}

func (o *SubscribeToStreamOptions) deadline() *time.Duration {
	return o.Deadline
}

func (o *SubscribeToStreamOptions) setDefaults() {
	if o.From == nil {
		o.From = End{}
	}
}

// SubscribeToAllOptions options of the subscribe to $all request.
type SubscribeToAllOptions struct {
	// Starting position of the subscribe request.
	From AllPosition
	// Whether the read request should resolve linkTo events to their linked events.
	ResolveLinkTos bool
	// Max search window.
	MaxSearchWindow int
	// Checkpoint interval.
	CheckpointInterval int
	// Applies a server-side filter to determine if an event of the subscription should be yielded.
	Filter *SubscriptionFilter
	// Asks for authenticated request.
	Authenticated *Credentials
	// A length of time to use for gRPC deadlines.
	Deadline *time.Duration
}

func (o *SubscribeToAllOptions) kind() operationKind {
	return streamingOperation
}

func (o *SubscribeToAllOptions) credentials() *Credentials {
	return o.Authenticated
}

func (o *SubscribeToAllOptions) deadline() *time.Duration {
	return o.Deadline
}

func (o *SubscribeToAllOptions) setDefaults() {
	if o.From == nil {
		o.From = End{}
	}

	if o.Filter != nil {
		if o.MaxSearchWindow == 0 {
			o.MaxSearchWindow = 32
		}

		if o.CheckpointInterval == 0 {
			o.CheckpointInterval = 1
		}
	}
}
