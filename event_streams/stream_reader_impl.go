package event_streams

import (
	"context"
	"io"
	"sync"

	"github.com/pivonroll/EventStore-Client-Go/connection"
	"github.com/pivonroll/EventStore-Client-Go/errors"
	"github.com/pivonroll/EventStore-Client-Go/protos/streams2"
)

type StreamReaderImpl struct {
	protoClient         streams2.Streams_ReadClient
	readResponseAdapter readResponseAdapter
	cancelFunc          context.CancelFunc
	once                sync.Once
}

func (this *StreamReaderImpl) ReadOne() (ReadResponse, errors.Error) {
	protoResponse, protoErr := this.protoClient.Recv()
	if protoErr != nil {
		if protoErr == io.EOF {
			return ReadResponse{}, errors.NewError(errors.EndOfStream, protoErr)
		}
		trailer := this.protoClient.Trailer()
		err := connection.GetErrorFromProtoException(trailer, protoErr)
		if err != nil {
			return ReadResponse{}, err
		}
		return ReadResponse{}, errors.NewError(errors.FatalError, protoErr)
	}

	result := this.readResponseAdapter.Create(protoResponse)
	return result, nil
}

func (this *StreamReaderImpl) Close() {
	this.once.Do(this.cancelFunc)
}

func newReadClientImpl(
	protoClient streams2.Streams_ReadClient,
	cancelFunc context.CancelFunc,
	readResponseAdapter readResponseAdapter) *StreamReaderImpl {
	return &StreamReaderImpl{
		protoClient:         protoClient,
		readResponseAdapter: readResponseAdapter,
		cancelFunc:          cancelFunc,
	}
}
