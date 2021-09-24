package event_streams

import "github.com/pivonroll/EventStore-Client-Go/protos/streams2"

type BatchAppendClientImpl struct {
	protoClient          streams2.Streams_BatchAppendClient
	batchResponseAdapter batchResponseAdapter
}

func (this BatchAppendClientImpl) Send(request BatchAppendRequest) error {
	err := this.protoClient.Send(request.Build())
	if err != nil {
		return err
	}

	return nil
}

func (this BatchAppendClientImpl) Recv() (BatchAppendResponse, error) {
	protoResponse, err := this.protoClient.Recv()
	if err != nil {
		return BatchAppendResponse{}, err
	}

	result := this.batchResponseAdapter.CreateResponse(protoResponse)
	return result, nil
}
