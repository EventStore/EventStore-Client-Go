package event_streams

import (
	"context"
	"io"
	"sync"

	"github.com/pivonroll/EventStore-Client-Go/connection"
	"github.com/pivonroll/EventStore-Client-Go/errors"
	"github.com/pivonroll/EventStore-Client-Go/protos/streams2"
)

// streamReaderImpl implements a StreamReader interface.
// Read is implemented with request/response mechanism which guarantees that
// go routine which initiated a read is receiving the result.
type streamReaderImpl struct {
	protoClient         streams2.Streams_ReadClient
	readResponseAdapter readResponseAdapter
	cancelFunc          context.CancelFunc
	once                sync.Once
	readRequestChannel  chan chan readResult
}

type readResult struct {
	ReadResponse
	errors.Error
}

// ReadOne issues a read request to a dedicated go routine and blocks until a response is received.
func (this *streamReaderImpl) ReadOne() (ReadResponse, errors.Error) {
	channel := make(chan readResult)

	this.readRequestChannel <- channel
	resp := <-channel

	return resp.ReadResponse, resp.Error
}

func (this *streamReaderImpl) readLoop() {
	for {
		responseChannel := <-this.readRequestChannel
		result, err := this.readOne()

		response := readResult{
			ReadResponse: result,
			Error:        err,
		}

		responseChannel <- response
	}
}

func (this *streamReaderImpl) readOne() (ReadResponse, errors.Error) {
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

	result, err := this.readResponseAdapter.create(protoResponse)
	return result, err
}

func (this *streamReaderImpl) Close() {
	this.once.Do(this.cancelFunc)
}

func newReadClientImpl(
	protoClient streams2.Streams_ReadClient,
	cancelFunc context.CancelFunc,
	readResponseAdapter readResponseAdapter) *streamReaderImpl {
	reader := &streamReaderImpl{
		protoClient:         protoClient,
		readResponseAdapter: readResponseAdapter,
		cancelFunc:          cancelFunc,
		readRequestChannel:  make(chan chan readResult),
	}

	go reader.readLoop()
	return reader
}
