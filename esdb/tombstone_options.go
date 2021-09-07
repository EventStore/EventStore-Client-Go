package esdb

type TombstoneStreamOptions struct {
	ExpectedRevision ExpectedRevision
	Authenticated    *Credentials
}

func (o *TombstoneStreamOptions) setDefaults() {
	if o.ExpectedRevision == nil {
		o.ExpectedRevision = Any{}
	}
}
