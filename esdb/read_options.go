package esdb

import (
	"time"
)

type ReadStreamOptions struct {
	Direction      Direction
	From           StreamPosition
	ResolveLinkTos bool
	Authenticated  *Credentials
	Deadline       *time.Duration
}

func (o *ReadStreamOptions) kind() operationKind {
	return StreamingOperation
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

type ReadAllOptions struct {
	Direction      Direction
	From           AllPosition
	ResolveLinkTos bool
	Authenticated  *Credentials
	Deadline       *time.Duration
}

func (o *ReadAllOptions) kind() operationKind {
	return StreamingOperation
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
