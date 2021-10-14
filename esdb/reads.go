package esdb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	api "github.com/EventStore/EventStore-Client-Go/protos/streams"
	"google.golang.org/grpc/metadata"
)

type readResp struct {
	event *ResolvedEvent
	err   *error
}

type ReadStream struct {
	client  *grpcClient
	channel chan (chan readResp)
	cancel  context.CancelFunc
	once    *sync.Once
}

type readStreamParams struct {
	client   *grpcClient
	handle   connectionHandle
	cancel   context.CancelFunc
	inner    api.Streams_ReadClient
	headers  metadata.MD
	trailers metadata.MD
}

func (stream *ReadStream) Close() {
	stream.once.Do(stream.cancel)
}

func (stream *ReadStream) Recv() (*ResolvedEvent, error) {
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

func newReadStream(params readStreamParams, firstEvt ResolvedEvent) *ReadStream {
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
		cachedEvent := &firstEvt
		for {
			resp := <-channel

			if cachedEvent != nil {
				resp <- readResp{
					event: cachedEvent,
				}

				cachedEvent = nil
				continue
			}

			if lastError != nil {
				resp <- readResp{
					err: lastError,
				}

				continue
			}

			result, err := params.inner.Recv()

			if err != nil {
				if !errors.Is(err, io.EOF) {
					err = params.client.handleError(params.handle, params.headers, params.trailers, err)
				}

				lastError = &err

				resp <- readResp{
					err: &err,
				}

				continue
			}

			resolvedEvent := getResolvedEventFromProto(result.GetEvent())
			resp <- readResp{
				event: &resolvedEvent,
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
