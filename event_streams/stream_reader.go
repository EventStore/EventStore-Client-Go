package event_streams

import (
	"context"

	"github.com/pivonroll/EventStore-Client-Go/errors"
	"github.com/pivonroll/EventStore-Client-Go/protos/streams2"
)

// StreamReader is an interface which represents a reader of a stream.
// Implementation of this interface should read a stream in a dedicated go routine to avoid
// issues which can occur when multiple go routines are trying to read data from protobuf stream.
type StreamReader interface {
	// ReadOne reads one message from a stream.
	// Message can be read after a successful read/subscription is
	// established with EventStoreDB's event stream.
	// Message must contain either an event or a checkpoint.
	// If message contains subscription confirmation or a stream-not-found this method must panic.
	ReadOne() (ReadResponse, errors.Error)
	// Close closes a protobuf stream used to read or subscribe to stream's events.
	Close()
}

// streamReaderFactory is used to construct a new event stream reader.
type streamReaderFactory interface {
	create(
		protoClient streams2.Streams_ReadClient,
		cancelFunc context.CancelFunc) StreamReader
}

type streamReaderFactoryImpl struct{}

func (this streamReaderFactoryImpl) create(
	protoClient streams2.Streams_ReadClient,
	cancelFunc context.CancelFunc) StreamReader {
	return newReadClientImpl(
		protoClient,
		cancelFunc,
		readResponseAdapterImpl{})
}
