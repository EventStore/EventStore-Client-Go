package event_streams

import (
	"context"
	"io"
	"sync"

	"github.com/EventStore/EventStore-Client-Go/connection"
	"github.com/EventStore/EventStore-Client-Go/errors"
	"github.com/EventStore/EventStore-Client-Go/protos/streams2"
)

type ReadClientImpl struct {
	protoClient         streams2.Streams_ReadClient
	readResponseAdapter readResponseAdapter
	cancelFunc          context.CancelFunc
	once                sync.Once
}

func (this *ReadClientImpl) Recv() (ReadResponse, errors.Error) {
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

func (this *ReadClientImpl) Close() {
	this.once.Do(this.cancelFunc)
}

func newReadClientImpl(
	protoClient streams2.Streams_ReadClient,
	cancelFunc context.CancelFunc,
	readResponseAdapter readResponseAdapter) *ReadClientImpl {
	return &ReadClientImpl{
		protoClient:         protoClient,
		readResponseAdapter: readResponseAdapter,
		cancelFunc:          cancelFunc,
	}
}
