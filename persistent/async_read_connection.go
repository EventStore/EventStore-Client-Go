package persistent

type AsyncReadConnection interface {
	RegisterHandler(handler EventAppearedHandler)
	Start(retryCount uint8) error
	Stop()
}
