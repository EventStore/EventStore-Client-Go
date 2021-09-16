package event_streams

import "github.com/EventStore/EventStore-Client-Go/protos/streams2"

type ReadClientImpl struct {
	protoClient         streams2.Streams_ReadClient
	readResponseAdapter readResponseAdapter
	streamId            string
}

func (this *ReadClientImpl) Recv() (ReadResponse, error) {
	protoResponse, err := this.protoClient.Recv()
	if err != nil {
		return ReadResponse{}, err
	}

	result := this.readResponseAdapter.Create(protoResponse)
	return result, nil
}

func newReadClientImpl(
	protoClient streams2.Streams_ReadClient,
	readResponseAdapter readResponseAdapter, streamId string) *ReadClientImpl {
	return &ReadClientImpl{
		protoClient:         protoClient,
		readResponseAdapter: readResponseAdapter,
		streamId:            streamId,
	}
}
