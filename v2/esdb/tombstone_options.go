package esdb

import "time"

type TombstoneStreamOptions struct {
	ExpectedRevision ExpectedRevision
	Authenticated    *Credentials
	Deadline         *time.Duration
}

func (o *TombstoneStreamOptions) kind() operationKind {
	return RegularOperation
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
