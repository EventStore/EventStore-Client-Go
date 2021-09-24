package event_streams

import (
	"context"

	"github.com/pivonroll/EventStore-Client-Go/errors"
	"github.com/pivonroll/EventStore-Client-Go/protos/streams2"
)

type StreamReader interface {
	ReadOne() (ReadResponse, errors.Error)
	Close()
}

type StreamReaderFactory interface {
	Create(
		protoClient streams2.Streams_ReadClient,
		cancelFunc context.CancelFunc) StreamReader
}

type StreamReaderFactoryImpl struct{}

func (this StreamReaderFactoryImpl) Create(
	protoClient streams2.Streams_ReadClient,
	cancelFunc context.CancelFunc) StreamReader {
	return newReadClientImpl(
		protoClient,
		cancelFunc,
		readResponseAdapterImpl{})
}
