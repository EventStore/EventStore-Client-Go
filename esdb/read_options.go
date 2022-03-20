package esdb

type ReadStreamOptions struct {
	Direction      Direction
	From           StreamPosition
	ResolveLinkTos bool
	Authenticated  *Credentials
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
}

func (o *ReadAllOptions) setDefaults() {
	if o.From == nil {
		o.From = Start{}
	}
}
