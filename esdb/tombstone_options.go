package esdb

import "time"

// TombstoneStreamOptions options of the tombstone stream request.
type TombstoneStreamOptions struct {
	// Asks the server to check that the stream receiving the event is at the given expected version.
	ExpectedRevision ExpectedRevision
	// Asks for authenticated request.
	Authenticated *Credentials
	// A length of time to use for gRPC deadlines.
	Deadline *time.Duration
}

func (o *TombstoneStreamOptions) kind() operationKind {
	return regularOperation
}

func (o *TombstoneStreamOptions) credentials() *Credentials {
	return o.Authenticated
}

func (o *TombstoneStreamOptions) deadline() *time.Duration {
	return o.Deadline
}

func (o *TombstoneStreamOptions) setDefaults() {
	if o.ExpectedRevision == nil {
		o.ExpectedRevision = Any{}
	}
}
