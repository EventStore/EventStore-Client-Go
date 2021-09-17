package event_streams

import (
	"context"
	"sync"

	"github.com/EventStore/EventStore-Client-Go/protos/streams2"
)

type ReadClientImpl struct {
	protoClient         streams2.Streams_ReadClient
	readResponseAdapter readResponseAdapter
	streamId            string
	cancelFunc          context.CancelFunc
	once                sync.Once
}

func (this *ReadClientImpl) Recv() (ReadResponse, error) {
	protoResponse, err := this.protoClient.Recv()
	if err != nil {
		return ReadResponse{}, err
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
	streamId string,
	readResponseAdapter readResponseAdapter) *ReadClientImpl {
	return &ReadClientImpl{
		protoClient:         protoClient,
		readResponseAdapter: readResponseAdapter,
		streamId:            streamId,
		cancelFunc:          cancelFunc,
	}
}
