package event_streams

import (
	"context"
	"io"

	"github.com/pivonroll/EventStore-Client-Go/connection"
	"github.com/pivonroll/EventStore-Client-Go/errors"
	"github.com/pivonroll/EventStore-Client-Go/protos/streams2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type ClientImpl struct {
	grpcClient               connection.GrpcClient
	deleteResponseAdapter    deleteResponseAdapter
	tombstoneResponseAdapter tombstoneResponseAdapter
	readClientFactory        StreamReaderFactory
	appendResponseAdapter    appendResponseAdapter
	readResponseAdapter      readResponseAdapter
}

const (
	AppendToStream_FailedToObtainAppenderErr errors.ErrorCode = "AppendToStream_FailedToObtainAppenderErr"
	AppendToStream_FailedSendHeaderErr       errors.ErrorCode = "AppendToStream_FailedSendHeaderErr"
	AppendToStream_FailedSendMessageErr      errors.ErrorCode = "AppendToStream_FailedSendMessageErr"
	AppendToStream_FailedToCloseStreamErr    errors.ErrorCode = "AppendToStream_FailedToCloseStreamErr"
)

func (client *ClientImpl) appendToStream(
	ctx context.Context,
	options AppendRequestContentOptions,
	events []ProposedEvent,
) (AppendResponse, errors.Error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return AppendResponse{}, err
	}

	grpcStreamsClient := streams2.NewStreamsClient(handle.Connection())

	var headers, trailers metadata.MD
	appendClient, protoErr := grpcStreamsClient.Append(ctx,
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, protoErr,
			AppendToStream_FailedToObtainAppenderErr)
		return AppendResponse{}, err
	}

	headerRequest := AppendRequest{Content: options}
	protoErr = appendClient.Send(headerRequest.Build())

	if protoErr != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, protoErr,
			AppendToStream_FailedSendHeaderErr)
		return AppendResponse{}, err
	}

	for _, event := range events {
		message := AppendRequest{Content: event.ToProposedMessage()}
		protoErr = appendClient.Send(message.Build())

		if protoErr != nil {
			err = client.grpcClient.HandleError(handle, headers, trailers, protoErr,
				AppendToStream_FailedSendMessageErr)
			return AppendResponse{}, err
		}
	}

	response, protoErr := appendClient.CloseAndRecv()
	if protoErr != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, protoErr,
			AppendToStream_FailedToCloseStreamErr)
		return AppendResponse{}, err
	}

	return client.appendResponseAdapter.CreateResponse(response), nil
}

func (client *ClientImpl) AppendToStream(
	ctx context.Context,
	streamID string,
	expectedStreamRevision IsAppendRequestExpectedStreamRevision,
	events []ProposedEvent,
) (AppendResponse, errors.Error) {
	return client.appendToStream(ctx, AppendRequestContentOptions{
		StreamIdentifier:       streamID,
		ExpectedStreamRevision: expectedStreamRevision,
	}, events)
}

func (client *ClientImpl) SetStreamMetadata(
	ctx context.Context,
	streamID string,
	expectedStreamRevision IsAppendRequestExpectedStreamRevision,
	metadata StreamMetadata) (AppendResponse, errors.Error) {
	streamMetadataEvent := NewMetadataEvent(metadata)

	return client.appendToStream(ctx, AppendRequestContentOptions{
		StreamIdentifier:       GetMetaStreamOf(streamID),
		ExpectedStreamRevision: expectedStreamRevision,
	}, []ProposedEvent{streamMetadataEvent})
}

const FailedToDeleteStreamErr errors.ErrorCode = "FailedToDeleteStreamErr"

func (client *ClientImpl) deleteStream(
	context context.Context,
	deleteRequest DeleteRequest,
) (DeleteResponse, errors.Error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return DeleteResponse{}, err
	}

	grpcStreamsClient := streams2.NewStreamsClient(handle.Connection())

	var headers, trailers metadata.MD
	deleteResponse, protoErr := grpcStreamsClient.Delete(context, deleteRequest.Build(),
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, protoErr, FailedToDeleteStreamErr)
		return DeleteResponse{}, err
	}

	return client.deleteResponseAdapter.Create(deleteResponse), nil
}

func (client *ClientImpl) DeleteStream(
	ctx context.Context,
	streamID string,
	revision IsDeleteRequestExpectedStreamRevision) (DeleteResponse, errors.Error) {
	return client.deleteStream(ctx, DeleteRequest{
		StreamIdentifier:       streamID,
		ExpectedStreamRevision: revision,
	})
}

func (client *ClientImpl) TombstoneStream(
	ctx context.Context,
	streamID string,
	revision IsTombstoneRequestExpectedStreamRevision) (TombstoneResponse, errors.Error) {
	return client.tombstoneStream(ctx, TombstoneRequest{
		StreamIdentifier:       streamID,
		ExpectedStreamRevision: revision,
	})
}

const FailedToTombstoneStreamErr errors.ErrorCode = "FailedToTombstoneStreamErr"

func (client *ClientImpl) tombstoneStream(
	context context.Context,
	tombstoneRequest TombstoneRequest,
) (TombstoneResponse, errors.Error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return TombstoneResponse{}, err
	}

	grpcStreamsClient := streams2.NewStreamsClient(handle.Connection())

	var headers, trailers metadata.MD
	tombstoneResponse, protoErr := grpcStreamsClient.Tombstone(context, tombstoneRequest.Build(),
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, protoErr, FailedToTombstoneStreamErr)
		return TombstoneResponse{}, err
	}

	return client.tombstoneResponseAdapter.Create(tombstoneResponse), nil
}

const (
	FailedToConstructReadStreamErr errors.ErrorCode = "FailedToConstructReadStreamErr"
)

func (client *ClientImpl) readStreamEvents(
	ctx context.Context,
	readRequest ReadRequest) ([]ReadResponseEvent, errors.Error) {

	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}

	grpcStreamsClient := streams2.NewStreamsClient(handle.Connection())

	var headers, trailers metadata.MD
	ctx, cancel := context.WithCancel(ctx)
	readStreamClient, protoErr := grpcStreamsClient.Read(ctx, readRequest.Build(),
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		defer cancel()
		err = client.grpcClient.HandleError(handle, headers, trailers, protoErr,
			FailedToConstructReadStreamErr)
		return nil, err
	}

	var result []ReadResponseEvent

	var readError errors.Error

	for {
		protoReadResult, protoError := readStreamClient.Recv()
		if protoError != nil {
			if protoError == io.EOF {
				break
			}
			err = client.grpcClient.HandleError(handle, headers, trailers, protoError)
			readError = err
			break
		}

		readResult := client.readResponseAdapter.Create(protoReadResult)
		if _, streamIsNotFound := readResult.GetStreamNotFound(); streamIsNotFound {
			readError = errors.NewErrorCode(errors.StreamNotFoundErr)
			break
		} else if _, isCheckpoint := readResult.GetCheckpoint(); isCheckpoint {
			continue
		}

		event, _ := readResult.GetEvent()
		result = append(result, event)
	}

	if readError == nil {
		defer cancel()
		return result, nil
	}

	defer cancel()
	return nil, readError
}

func (client *ClientImpl) GetStreamReader(
	ctx context.Context,
	streamID string,
	direction ReadRequestDirection,
	revision IsReadRequestStreamOptionsStreamRevision,
	count uint64,
	resolveLinks bool) (StreamReader, errors.Error) {
	return client.getStreamEventsReader(ctx, ReadRequest{
		StreamOption: ReadRequestStreamOptions{
			StreamIdentifier: streamID,
			Revision:         revision,
		},
		Direction:    direction,
		ResolveLinks: resolveLinks,
		Count:        count,
		Filter:       ReadRequestNoFilter{},
	})
}

func (client *ClientImpl) GetAllEventsReader(
	ctx context.Context,
	direction ReadRequestDirection,
	position IsReadRequestOptionsAllPosition,
	count uint64,
	resolveLinks bool,
) (StreamReader, errors.Error) {
	return client.getStreamEventsReader(ctx, ReadRequest{
		StreamOption: ReadRequestStreamOptionsAll{
			Position: position,
		},
		Direction:    direction,
		ResolveLinks: resolveLinks,
		Count:        count,
		Filter:       ReadRequestNoFilter{},
	})
}

func (client *ClientImpl) getStreamEventsReader(
	ctx context.Context,
	readRequest ReadRequest) (StreamReader, errors.Error) {

	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}

	grpcStreamsClient := streams2.NewStreamsClient(handle.Connection())

	var headers, trailers metadata.MD
	ctx, cancel := context.WithCancel(ctx)
	readStreamClient, protoErr := grpcStreamsClient.Read(ctx, readRequest.Build(),
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		defer cancel()
		err = client.grpcClient.HandleError(handle, headers, trailers, protoErr,
			FailedToConstructReadStreamErr)
		return nil, err
	}

	readClient := client.readClientFactory.Create(readStreamClient, cancel)
	return readClient, nil
}

func (client *ClientImpl) SubscribeToStream(
	ctx context.Context,
	streamID string,
	revision IsSubscribeRequestStreamOptionsStreamRevision,
	resolveLinks bool,
) (StreamReader, errors.Error) {
	return client.subscribeToStream(ctx, SubscribeToStreamRequest{
		StreamOption: SubscribeRequestStreamOptions{
			StreamIdentifier: streamID,
			Revision:         revision,
		},
		Direction:    SubscribeRequestDirectionForward,
		ResolveLinks: resolveLinks,
		Filter:       SubscribeRequestNoFilter{},
	})
}

func (client *ClientImpl) SubscribeToAllFiltered(
	ctx context.Context,
	position IsSubscribeRequestOptionsAllPosition,
	resolveLinks bool,
	filter SubscribeRequestFilter,
) (StreamReader, errors.Error) {
	return client.subscribeToStream(ctx, SubscribeToStreamRequest{
		StreamOption: SubscribeRequestStreamOptionsAll{
			Position: position,
		},
		Direction:    SubscribeRequestDirectionForward,
		ResolveLinks: resolveLinks,
		Filter:       filter,
	})
}

func (client *ClientImpl) SubscribeToAll(
	ctx context.Context,
	position IsSubscribeRequestOptionsAllPosition,
	resolveLinks bool,
) (StreamReader, errors.Error) {
	return client.subscribeToStream(ctx, SubscribeToStreamRequest{
		StreamOption: SubscribeRequestStreamOptionsAll{
			Position: position,
		},
		Direction:    SubscribeRequestDirectionForward,
		ResolveLinks: resolveLinks,
		Filter:       SubscribeRequestNoFilter{},
	})
}

const (
	FailedToCreateReaderErr                errors.ErrorCode = "FailedToCreateReaderErr"
	FailedToReceiveSubscriptionResponseErr errors.ErrorCode = "FailedToReceiveSubscriptionResponseErr"
)

func (client *ClientImpl) subscribeToStream(
	ctx context.Context,
	request SubscribeToStreamRequest,
) (StreamReader, errors.Error) {
	var headers, trailers metadata.MD

	ctx, cancel := context.WithCancel(ctx)
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		defer cancel()
		return nil, err
	}

	grpcStreamsClient := streams2.NewStreamsClient(handle.Connection())

	readStreamClient, protoErr := grpcStreamsClient.Read(ctx, request.Build(),
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		defer cancel()
		err := client.grpcClient.HandleError(handle, headers, trailers, protoErr, FailedToCreateReaderErr)
		return nil, err
	}

	readResult, protoErr := readStreamClient.Recv()
	if protoErr != nil {
		defer cancel()
		err := client.grpcClient.HandleError(handle, headers, trailers, protoErr,
			FailedToReceiveSubscriptionResponseErr)
		return nil, err
	}

	switch readResult.Content.(type) {
	case *streams2.ReadResp_Confirmation:
		{
			readClient := client.readClientFactory.Create(readStreamClient, cancel)
			return readClient, nil
		}
	case *streams2.ReadResp_StreamNotFound_:
		{
			defer cancel()
			return nil, errors.NewErrorCode(errors.StreamNotFoundErr)
		}
	}
	defer cancel()
	return nil, errors.NewErrorCode(errors.FatalError)
}

func (client *ClientImpl) GetStreamMetadata(
	ctx context.Context,
	streamID string) (StreamMetadataResult, errors.Error) {

	events, err := client.readStreamEvents(ctx, ReadRequest{
		StreamOption: ReadRequestStreamOptions{
			StreamIdentifier: GetMetaStreamOf(streamID),
			Revision:         ReadRequestOptionsStreamRevisionEnd{},
		},
		Direction:    ReadRequestDirectionBackward,
		ResolveLinks: false,
		Count:        1,
		Filter:       ReadRequestNoFilter{},
	})
	if err != nil {
		if err.Code() == errors.StreamNotFoundErr {
			return StreamMetadataNone{}, nil
		}
		return StreamMetadataNone{}, err
	}

	if len(events) == 0 {
		return StreamMetadataNone{}, nil
	}

	return NewStreamMetadataResultImpl(streamID, events[0]), nil
}

func (client *ClientImpl) ReadStreamEvents(
	ctx context.Context,
	streamID string,
	direction ReadRequestDirection,
	revision IsReadRequestStreamOptionsStreamRevision,
	count uint64,
	resolveLinks bool) (ReadResponseEventList, errors.Error) {
	return client.readStreamEvents(ctx, ReadRequest{
		StreamOption: ReadRequestStreamOptions{
			StreamIdentifier: streamID,
			Revision:         revision,
		},
		Direction:    direction,
		ResolveLinks: resolveLinks,
		Count:        count,
		Filter:       ReadRequestNoFilter{},
	})
}

func (client *ClientImpl) ReadAllEvents(
	ctx context.Context,
	direction ReadRequestDirection,
	position IsReadRequestOptionsAllPosition,
	count uint64,
	resolveLinks bool,
) (ReadResponseEventList, errors.Error) {

	return client.readStreamEvents(ctx, ReadRequest{
		StreamOption: ReadRequestStreamOptionsAll{
			Position: position,
		},
		Direction:    direction,
		ResolveLinks: resolveLinks,
		Count:        count,
		Filter:       ReadRequestNoFilter{},
	})
}
