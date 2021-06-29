package persistent

type messageAdapterProvider interface {
	GetMessageAdapter() messageAdapter
}

type messageAdapterProviderImpl struct{}

func (provider messageAdapterProviderImpl) GetMessageAdapter() messageAdapter {
	return messageAdapterImpl{}
}
