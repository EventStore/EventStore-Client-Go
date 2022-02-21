package esdb

import (
	"time"
)

type SubscribeToStreamOptions struct {
	From           StreamPosition
	ResolveLinkTos bool
	Authenticated  *Credentials
	Deadline       *time.Duration
}

func (o *SubscribeToStreamOptions) kind() operationKind {
	return StreamingOperation
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

type SubscribeToAllOptions struct {
	From               AllPosition
	ResolveLinkTos     bool
	MaxSearchWindow    int
	CheckpointInterval int
	Filter             *SubscriptionFilter
	Authenticated      *Credentials
	Deadline           *time.Duration
}

func (o *SubscribeToAllOptions) kind() operationKind {
	return StreamingOperation
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
