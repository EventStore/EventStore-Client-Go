package esdb

import (
	"time"
)

type AppendToStreamOptions struct {
	ExpectedRevision ExpectedRevision
	Authenticated    *Credentials
	Deadline         *time.Duration
}

func (o *AppendToStreamOptions) kind() operationKind {
	return RegularOperation
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
