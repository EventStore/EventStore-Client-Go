package esdb

import (
	"time"
)

// ReadStreamOptions options of the read stream request.
type ReadStreamOptions struct {
	// Direction of the read request.
	Direction Direction
	// Starting position of the read request.
	From StreamPosition
	// Whether the read request should resolve linkTo events to their linked events.
	ResolveLinkTos bool
	// Asks for authenticated request.
	Authenticated *Credentials
	// A length of time to use for gRPC deadlines.
	Deadline *time.Duration
}

func (o *ReadStreamOptions) kind() operationKind {
	return streamingOperation
}

func (o *ReadStreamOptions) credentials() *Credentials {
	return o.Authenticated
}

func (o *ReadStreamOptions) deadline() *time.Duration {
	return o.Deadline
}

func (o *ReadStreamOptions) setDefaults() {
	if o.From == nil {
		o.From = Start{}
	}
}

// ReadAllOptions options of the read $all request.
type ReadAllOptions struct {
	// Direction of the read request.
	Direction Direction
	// Starting position of the read request.
	From AllPosition
	// Whether the read request should resolve linkTo events to their linked events.
	ResolveLinkTos bool
	// Asks for authenticated request.
	Authenticated *Credentials
	// A length of time to use for gRPC deadlines.
	Deadline *time.Duration
}

func (o *ReadAllOptions) kind() operationKind {
	return streamingOperation
}

func (o *ReadAllOptions) credentials() *Credentials {
	return o.Authenticated
}

func (o *ReadAllOptions) deadline() *time.Duration {
	return o.Deadline
}

func (o *ReadAllOptions) setDefaults() {
	if o.From == nil {
		o.From = Start{}
	}
}
