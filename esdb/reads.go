package esdb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	api "github.com/EventStore/EventStore-Client-Go/v2/protos/streams"
	"google.golang.org/grpc/metadata"
)

// ReadStream read stream iterator.
type ReadStream struct {
	once   *sync.Once
	closed *int32
	params readStreamParams
}

type readStreamParams struct {
	client   *grpcClient
	handle   *connectionHandle
	cancel   context.CancelFunc
	inner    api.Streams_ReadClient
	headers  *metadata.MD
	trailers *metadata.MD
}

// Close closes the iterator and release allocated resources.
func (stream *ReadStream) Close() {
	stream.once.Do(func() {
		atomic.StoreInt32(stream.closed, 1)
		stream.params.cancel()
	})
}

// Recv awaits for the next incoming event.
func (stream *ReadStream) Recv() (*ResolvedEvent, error) {
	if atomic.LoadInt32(stream.closed) != 0 {
		return nil, io.EOF
	}

	msg, err := stream.params.inner.Recv()

	if err != nil {
		atomic.StoreInt32(stream.closed, 1)

		if !errors.Is(err, io.EOF) {
			err = stream.params.client.handleError(stream.params.handle, *stream.params.headers, *stream.params.trailers, err)
		}

		return nil, err
	}

	switch msg.Content.(type) {
	case *api.ReadResp_Event:
		resolvedEvent := getResolvedEventFromProto(msg.GetEvent())
		return &resolvedEvent, nil
	case *api.ReadResp_StreamNotFound_:
		atomic.StoreInt32(stream.closed, 1)
		streamName := string(msg.Content.(*api.ReadResp_StreamNotFound_).StreamNotFound.StreamIdentifier.StreamName)
		return nil, &Error{code: ErrorCodeResourceNotFound, err: fmt.Errorf("stream '%s' is not found", streamName)}
	}

	panic("unreachable code")
}

func newReadStream(params readStreamParams) *ReadStream {
	once := new(sync.Once)
	closed := new(int32)

	atomic.StoreInt32(closed, 0)

	return &ReadStream{
		once:   once,
		closed: closed,
		params: params,
	}
}
