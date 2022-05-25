package esdb

import (
	"time"
)

// AppendToStreamOptions options of the append stream request.
type AppendToStreamOptions struct {
	// Asks the server to check that the stream receiving the event is at the given expected version.
	ExpectedRevision ExpectedRevision
	// Asks for authenticated request.
	Authenticated *Credentials
	// A length of time to use for gRPC deadlines.
	Deadline *time.Duration
}

func (o *AppendToStreamOptions) kind() operationKind {
	return regularOperation
}

func (o *AppendToStreamOptions) credentials() *Credentials {
	return o.Authenticated
}

func (o *AppendToStreamOptions) deadline() *time.Duration {
	return o.Deadline
}

func (o *AppendToStreamOptions) setDefaults() {
	if o.ExpectedRevision == nil {
		o.ExpectedRevision = Any{}
	}
}
