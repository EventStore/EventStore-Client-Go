package event_streams

import "github.com/pivonroll/EventStore-Client-Go/protos/streams2"

type BatchAppender interface {
	Send(BatchAppendRequest) error
	Recv() (BatchAppendResponse, error)
}

type BatchAppendClientFactory interface {
	Create(protoClient streams2.Streams_BatchAppendClient) BatchAppender
}

type BatchAppendClientFactoryImpl struct{}

func (this BatchAppendClientFactoryImpl) Create(
	protoClient streams2.Streams_BatchAppendClient) BatchAppender {
	return newBatchAppendClientImpl(protoClient, batchResponseAdapterImpl{})
}

func newBatchAppendClientImpl(
	protoClient streams2.Streams_BatchAppendClient,
	impl batchResponseAdapter) *BatchAppendClientImpl {
	return &BatchAppendClientImpl{
		protoClient:          protoClient,
		batchResponseAdapter: impl,
	}
}
