package esdb

import (
	"time"
)

// DeleteStreamOptions options of the delete stream request.
type DeleteStreamOptions struct {
	// Asks the server to check that the stream receiving the event is at the given expected version.
	ExpectedRevision ExpectedRevision
	// Asks for authenticated request.
	Authenticated *Credentials
	// A length of time to use for gRPC deadlines.
	Deadline *time.Duration
}

func (o *DeleteStreamOptions) kind() operationKind {
	return regularOperation
}

func (o *DeleteStreamOptions) credentials() *Credentials {
	return o.Authenticated
}

func (o *DeleteStreamOptions) deadline() *time.Duration {
	return o.Deadline
}

func (o *DeleteStreamOptions) setDefaults() {
	if o.ExpectedRevision == nil {
		o.ExpectedRevision = Any{}
	}
}
