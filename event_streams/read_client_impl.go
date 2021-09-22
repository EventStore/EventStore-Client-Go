package event_streams

import (
	"context"
	"fmt"
	"io"
	"sync"

	"google.golang.org/grpc/metadata"

	"github.com/EventStore/EventStore-Client-Go/connection"
	"github.com/EventStore/EventStore-Client-Go/errors"
	"github.com/EventStore/EventStore-Client-Go/protos/streams2"
)

type ReadClientImpl struct {
	grpcClient          connection.GrpcClient
	handle              connection.ConnectionHandle
	headers             *metadata.MD
	trailers            *metadata.MD
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
		if protoErr == io.EOF {
			return ReadResponse{}, errors.NewError(EndOfStreamErr, protoErr)
		}
		fmt.Println("ProtoErr:", protoErr)
		err := this.grpcClient.HandleError(this.handle, *this.headers, *this.trailers, protoErr)
		fmt.Println("Err:", err)
		if err != nil {
			return ReadResponse{}, err
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
	handle connection.ConnectionHandle,
	headers *metadata.MD,
	trailers *metadata.MD,
	protoClient streams2.Streams_ReadClient,
	cancelFunc context.CancelFunc,
	streamId string,
	readResponseAdapter readResponseAdapter) *ReadClientImpl {
	return &ReadClientImpl{
		grpcClient:          grpcClient,
		handle:              handle,
		headers:             headers,
		trailers:            trailers,
		protoClient:         protoClient,
		readResponseAdapter: readResponseAdapter,
		streamId:            streamId,
		cancelFunc:          cancelFunc,
	}
}
