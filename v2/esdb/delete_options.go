package esdb

import (
	"time"
)

type DeleteStreamOptions struct {
	ExpectedRevision ExpectedRevision
	Authenticated    *Credentials
	Deadline         *time.Duration
}

func (o *DeleteStreamOptions) kind() operationKind {
	return RegularOperation
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
