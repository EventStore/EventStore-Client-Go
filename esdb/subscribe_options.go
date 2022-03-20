package esdb

type SubscribeToStreamOptions struct {
	From           StreamPosition
	ResolveLinkTos bool
	Authenticated  *Credentials
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
