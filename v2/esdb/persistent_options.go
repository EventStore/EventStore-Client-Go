package esdb

import "time"

type PersistentStreamSubscriptionOptions struct {
	Settings      *SubscriptionSettings
	StartFrom     StreamPosition
	Authenticated *Credentials
	Deadline      *time.Duration
}

func (o *PersistentStreamSubscriptionOptions) kind() operationKind {
	return RegularOperation
}

func (o *PersistentStreamSubscriptionOptions) credentials() *Credentials {
	return o.Authenticated
}

func (o *PersistentStreamSubscriptionOptions) deadline() *time.Duration {
	return o.Deadline
}

func (o *PersistentStreamSubscriptionOptions) setDefaults() {
	if o.StartFrom == nil {
		o.StartFrom = End{}
	}
}

type PersistentAllSubscriptionOptions struct {
	Settings        *SubscriptionSettings
	StartFrom       AllPosition
	MaxSearchWindow int
	Filter          *SubscriptionFilter
	Authenticated   *Credentials
	Deadline        *time.Duration
}

func (o *PersistentAllSubscriptionOptions) kind() operationKind {
	return RegularOperation
}

func (o *PersistentAllSubscriptionOptions) credentials() *Credentials {
	return o.Authenticated
}

func (o *PersistentAllSubscriptionOptions) deadline() *time.Duration {
	return o.Deadline
}

func (o *PersistentAllSubscriptionOptions) setDefaults() {
	if o.StartFrom == nil {
		o.StartFrom = End{}
	}

	if o.Filter != nil {
		if o.MaxSearchWindow == 0 {
			o.MaxSearchWindow = 32
		}
	}
}

type SubscribeToPersistentSubscriptionOptions struct {
	BufferSize    uint32
	Authenticated *Credentials
	Deadline      *time.Duration
}

func (o *SubscribeToPersistentSubscriptionOptions) kind() operationKind {
	return StreamingOperation
}

func (o *SubscribeToPersistentSubscriptionOptions) credentials() *Credentials {
	return o.Authenticated
}

func (o *SubscribeToPersistentSubscriptionOptions) deadline() *time.Duration {
	return o.Deadline
}

func (o *SubscribeToPersistentSubscriptionOptions) setDefaults() {
	if o.BufferSize == 0 {
		o.BufferSize = 10
	}
}

type DeletePersistentSubscriptionOptions struct {
	Authenticated *Credentials
	Deadline      *time.Duration
}

func (d DeletePersistentSubscriptionOptions) kind() operationKind {
	return RegularOperation
}

func (d DeletePersistentSubscriptionOptions) credentials() *Credentials {
	return d.Authenticated
}

func (d DeletePersistentSubscriptionOptions) deadline() *time.Duration {
	return d.Deadline
}

type ReplayParkedMessagesOptions struct {
	Authenticated *Credentials
	StopAt        int
	Deadline      *time.Duration
}

func (r ReplayParkedMessagesOptions) kind() operationKind {
	return RegularOperation
}

func (r ReplayParkedMessagesOptions) credentials() *Credentials {
	return r.Authenticated
}

func (r ReplayParkedMessagesOptions) deadline() *time.Duration {
	return r.Deadline
}

type ListPersistentSubscriptionsOptions struct {
	Authenticated *Credentials
	Deadline      *time.Duration
}

func (l ListPersistentSubscriptionsOptions) kind() operationKind {
	return RegularOperation
}

func (l ListPersistentSubscriptionsOptions) credentials() *Credentials {
	return l.Authenticated
}

func (l ListPersistentSubscriptionsOptions) deadline() *time.Duration {
	return l.Deadline
}

type GetPersistentSubscriptionOptions struct {
	Authenticated *Credentials
	Deadline      *time.Duration
}

func (g GetPersistentSubscriptionOptions) kind() operationKind {
	return RegularOperation
}

func (g GetPersistentSubscriptionOptions) credentials() *Credentials {
	return g.Authenticated
}

func (g GetPersistentSubscriptionOptions) deadline() *time.Duration {
	return g.Deadline
}

type RestartPersistentSubscriptionSubsystemOptions struct {
	Authenticated *Credentials
	Deadline      *time.Duration
}

func (g RestartPersistentSubscriptionSubsystemOptions) kind() operationKind {
	return RegularOperation
}

func (g RestartPersistentSubscriptionSubsystemOptions) credentials() *Credentials {
	return g.Authenticated
}

func (g RestartPersistentSubscriptionSubsystemOptions) deadline() *time.Duration {
	return g.Deadline
}
