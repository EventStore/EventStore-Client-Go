package esdb

type AppendToStreamOptions struct {
	ExpectedRevision ExpectedRevision
	Authenticated    *Credentials
}

func (o *AppendToStreamOptions) setDefaults() {
	if o.ExpectedRevision == nil {
		o.ExpectedRevision = Any{}
	}
}
