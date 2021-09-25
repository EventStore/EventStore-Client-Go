package buffered_async

import "github.com/pivonroll/EventStore-Client-Go/errors"

//go:generate mockgen -source=reader_helper.go -destination=reader_helper_mock.go -package=buffered_async

type readerHelper interface {
	Read() (interface{}, errors.Error)
}
