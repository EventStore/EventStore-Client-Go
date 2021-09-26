package buffered_async

type Reader interface {
	Start(readerFunc ReaderFunc) <-chan interface{}
	Stop()
}
