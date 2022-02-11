package esdb

type PersistentStreamSubscriptionOptions struct {
	Settings      *SubscriptionSettings
	StartFrom     StreamPosition
	Authenticated *Credentials
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
}

func (o *SubscribeToPersistentSubscriptionOptions) setDefaults() {
	if o.BufferSize == 0 {
		o.BufferSize = 10
	}
}

type DeletePersistentSubscriptionOptions struct {
	Authenticated *Credentials
}

type ReplayParkedMessagesOptions struct {
	Authenticated *Credentials
	StopAt        int
}

type ListPersistentSubscriptionsOptions struct {
	Authenticated *Credentials
}

type GetPersistentSubscriptionOptions struct {
	Authenticated *Credentials
}

type RestartPersistentSubscriptionSubsystemOptions struct {
	Authenticated *Credentials
}
