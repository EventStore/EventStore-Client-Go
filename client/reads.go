package client

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/EventStore/EventStore-Client-Go/connection"
	"github.com/EventStore/EventStore-Client-Go/errors"
	"github.com/EventStore/EventStore-Client-Go/internal/protoutils"
	"github.com/EventStore/EventStore-Client-Go/messages"
	api "github.com/EventStore/EventStore-Client-Go/protos/streams"
	"google.golang.org/grpc/metadata"
)

type readResp struct {
	event *messages.ResolvedEvent
	err   *error
}

type ReadStream struct {
	client  connection.GrpcClient
	channel chan (chan readResp)
	cancel  context.CancelFunc
	once    *sync.Once
}

type ReadStreamParams struct {
	client   connection.GrpcClient
	handle   connection.ConnectionHandle
	cancel   context.CancelFunc
	inner    api.Streams_ReadClient
	headers  metadata.MD
	trailers metadata.MD
}

func (stream *ReadStream) Close() {
	stream.once.Do(stream.cancel)
}

func (stream *ReadStream) Recv() (*messages.ResolvedEvent, error) {
	promise := make(chan readResp)

	stream.channel <- promise

	resp, isOk := <-promise

	if !isOk {
		return nil, fmt.Errorf("read stream has been termimated")
	}

	if resp.err != nil {
		return nil, *resp.err
	}

	return resp.event, nil
}

func NewReadStream(params ReadStreamParams) *ReadStream {
	channel := make(chan (chan readResp))
	once := new(sync.Once)

	// It is not safe to consume a stream in different goroutines. This is why we only consume
	// the stream in a dedicated goroutine.
	//
	// Current implementation doesn't terminate the goroutine. When a stream is terminated (without or with an error),
	// we keep user requests coming but will always send back the last errror messages we got.
	// This implementation is simple to maintain while letting the user sharing their subscription
	// among as many goroutines as they want.
	go func() {
		var lastError *error
		for {
			resp := <-channel

			if lastError != nil {
				resp <- readResp{
					err: lastError,
				}

				continue
			}

			result, err := params.inner.Recv()

			if err != nil {
				if err != io.EOF {
					err = params.client.HandleError(params.handle, params.headers, params.trailers, err)
				}

				lastError = &err

				resp <- readResp{
					err: &err,
				}

				continue
			}

			switch result.Content.(type) {
			case *api.ReadResp_Event:
				{
					resolvedEvent := protoutils.GetResolvedEventFromProto(result.GetEvent())
					resp <- readResp{
						event: &resolvedEvent,
					}
				}
			case *api.ReadResp_StreamNotFound_:
				{
					lastError = &errors.ErrStreamNotFound
					resp <- readResp{
						err: lastError,
					}
				}
			}
		}

	}()

	return &ReadStream{
		client:  params.client,
		channel: channel,
		once:    once,
		cancel:  params.cancel,
	}
}
