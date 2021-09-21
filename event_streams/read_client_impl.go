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
	grpcClient          connection.GrpcClient
	protoClient         streams2.Streams_ReadClient
	readResponseAdapter readResponseAdapter
	streamId            string
	cancelFunc          context.CancelFunc
	once                sync.Once
}

const EndOfStreamErr errors.ErrorCode = "EndOfStreamErr"

func (this *ReadClientImpl) Recv() (ReadResponse, errors.Error) {
	protoResponse, protoErr := this.protoClient.Recv()
	if protoErr != nil {
		err := connection.ErrorFromStdErrorByStatus(protoErr)
		if err != nil {
			return ReadResponse{}, err
		} else if protoErr == io.EOF {
			return ReadResponse{}, errors.NewError(EndOfStreamErr, protoErr)
		}
		return ReadResponse{}, errors.NewError(FatalError, protoErr)
	}

	result := this.readResponseAdapter.Create(protoResponse)
	return result, nil
}

func (this *ReadClientImpl) Close() {
	this.once.Do(this.cancelFunc)
}

func newReadClientImpl(
	grpcClient connection.GrpcClient,
	protoClient streams2.Streams_ReadClient,
	cancelFunc context.CancelFunc,
	streamId string,
	readResponseAdapter readResponseAdapter) *ReadClientImpl {
	return &ReadClientImpl{
		grpcClient:          grpcClient,
		protoClient:         protoClient,
		readResponseAdapter: readResponseAdapter,
		streamId:            streamId,
		cancelFunc:          cancelFunc,
	}
}
