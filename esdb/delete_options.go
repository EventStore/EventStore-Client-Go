package esdb

type DeleteStreamOptions struct {
	ExpectedRevision ExpectedRevision
	Authenticated    *Credentials
}

func (o *DeleteStreamOptions) setDefaults() {
	if o.ExpectedRevision == nil {
		o.ExpectedRevision = Any{}
	}
}
