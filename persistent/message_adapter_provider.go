package persistent

//go:generate mockgen -source=message_adapter_provider.go -destination=message_adapter_provider_mock.go -package=persistent

type messageAdapterProvider interface {
	GetMessageAdapter() messageAdapter
}

type messageAdapterProviderImpl struct{}

func (provider messageAdapterProviderImpl) GetMessageAdapter() messageAdapter {
	return messageAdapterImpl{}
}
