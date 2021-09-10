package buffered_async

//go:generate mockgen -source=reader_helper.go -destination=reader_helper_mock.go -package=buffered_async

type readerHelper interface {
	Read() (interface{}, error)
}
