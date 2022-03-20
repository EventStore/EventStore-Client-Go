package esdb

type PersistentStreamSubscriptionOptions struct {
	Settings      *SubscriptionSettings
	From          StreamPosition
	Authenticated *Credentials
}

func (o *PersistentStreamSubscriptionOptions) setDefaults() {
	if o.From == nil {
		o.From = End{}
	}
}

type PersistentAllSubscriptionOptions struct {
	Settings           *SubscriptionSettings
	From               AllPosition
	MaxSearchWindow    int
	CheckpointInterval int
	Filter             *SubscriptionFilter
	Authenticated      *Credentials
}

func (o *PersistentAllSubscriptionOptions) setDefaults() {
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

type ConnectToPersistentSubscriptionOptions struct {
	BatchSize     uint32
	Authenticated *Credentials
}

func (o *ConnectToPersistentSubscriptionOptions) setDefaults() {
	if o.BatchSize == 0 {
		o.BatchSize = 10
	}
}

type DeletePersistentSubscriptionOptions struct {
	Authenticated *Credentials
}
