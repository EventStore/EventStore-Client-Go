package event_streams

import (
	"context"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/pivonroll/EventStore-Client-Go/connection"
	"github.com/pivonroll/EventStore-Client-Go/errors"
	"github.com/pivonroll/EventStore-Client-Go/protos/streams2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// ClientImpl is an implementation of a Client interface which can interact with EventStoreDB streams.
type ClientImpl struct {
	grpcClient               connection.GrpcClient
	deleteResponseAdapter    deleteResponseAdapter
	tombstoneResponseAdapter tombstoneResponseAdapter
	readClientFactory        streamReaderFactory
	appendResponseAdapter    appendResponseAdapter
	readResponseAdapter      readResponseAdapter
	batchResponseAdapter     batchResponseAdapter
}

const (
	// AppendToStream_FailedToObtainWriterErr indicates that client failed to receive a protobuf append client.
	AppendToStream_FailedToObtainWriterErr errors.ErrorCode = "AppendToStream_FailedToObtainWriterErr"
	// AppendToStream_FailedSendHeaderErr indicates that client received an unknown error
	// when it tried to send a header to a protobuf stream.
	// Header is sent before client can append any events to a stream.
	AppendToStream_FailedSendHeaderErr errors.ErrorCode = "AppendToStream_FailedSendHeaderErr"
	// AppendToStream_FailedSendMessageErr indicates that there was an unknown error received when client
	// tried to append an event to a EventStoreDB stream.
	AppendToStream_FailedSendMessageErr errors.ErrorCode = "AppendToStream_FailedSendMessageErr"
	// AppendToStream_FailedToCloseStreamErr indicates that there was an unknown error when client
	// tried to close the protobuf stream after it has written all events to an EventStoreDB stream.
	AppendToStream_FailedToCloseStreamErr errors.ErrorCode = "AppendToStream_FailedToCloseStreamErr"
)

// AppendToStream appends a slice of events to a stream.
//
// Events are sent to a stream one by one.
//
// If appending of one event fails EventStoreDb will roll back the whole transaction.
//
// If any error occurs error will be returned with appropriate code set.
func (client *ClientImpl) AppendToStream(
	ctx context.Context,
	streamID string,
	expectedStreamRevision IsWriteStreamRevision,
	events []ProposedEvent,
) (AppendResponse, errors.Error) {
	return client.appendToStreamWithError(ctx, appendRequestContentOptions{
		streamId:               streamID,
		expectedStreamRevision: expectedStreamRevision,
	}, events)
}

const (
	// BatchAppendToStream_FailedToObtainWriterErr indicates that client failed to receive a protobuf append client.
	BatchAppendToStream_FailedToObtainWriterErr errors.ErrorCode = "BatchAppendToStream_FailedToObtainWriterErr"
	// BatchAppendToStream_FailedSendMessageErr indicates that there was an unknown error received when client
	// tried to append a chunk of events to a EventStoreDB stream.
	BatchAppendToStream_FailedSendMessageErr errors.ErrorCode = "BatchAppendToStream_FailedSendMessageErr"
	// BatchAppendToStream_FailedToCloseStreamErr indicates that there was an unknown error when client
	// tried to close the protobuf stream after it has written all event chunks to an EventStoreDB stream.
	BatchAppendToStream_FailedToCloseStreamErr errors.ErrorCode = "BatchAppendToStream_FailedToCloseStreamErr"
)

// BatchAppendToStream appends events to a stream in chunks.
//
// Correlation ID for events will be auto generated.
//
// If batch append of one chunk fails EventStoreDb will roll back the whole transaction.
//
// If any error occurs error will be returned with appropriate code set.
func (client *ClientImpl) BatchAppendToStream(ctx context.Context,
	streamId string,
	expectedStreamRevision IsWriteStreamRevision,
	events ProposedEventList,
	chunkSize uint64,
	deadline time.Time,
) (BatchAppendResponse, errors.Error) {
	correlationId, _ := uuid.NewRandom()
	return client.BatchAppendToStreamWithCorrelationId(ctx,
		streamId,
		expectedStreamRevision,
		correlationId,
		events,
		chunkSize,
		deadline)
}

// BatchAppendToStreamWithCorrelationId appends events to a stream in chunks.
//
// CorrelationId for events must be provided.
//
// If batch append of one chunk fails EventStoreDb will roll back the whole transaction.
//
// If any error occurs error will be returned with appropriate code set.
func (client *ClientImpl) BatchAppendToStreamWithCorrelationId(ctx context.Context,
	streamId string,
	expectedStreamRevision IsWriteStreamRevision,
	correlationId uuid.UUID,
	events ProposedEventList,
	chunkSize uint64,
	deadline time.Time,
) (BatchAppendResponse, errors.Error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return BatchAppendResponse{}, err
	}

	grpcStreamsClient := streams2.NewStreamsClient(handle.Connection())

	var headers, trailers metadata.MD
	appendClient, protoErr := grpcStreamsClient.BatchAppend(ctx,
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, protoErr,
			BatchAppendToStream_FailedToObtainWriterErr)
		return BatchAppendResponse{}, err
	}

	chunks := events.toBatchAppendRequestChunks(chunkSize)

	for index, chunk := range chunks {
		request := batchAppendRequest{
			correlationId: correlationId,
			options: batchAppendRequestOptions{
				streamId:               streamId,
				expectedStreamRevision: expectedStreamRevision,
				deadline:               deadline,
			},
			proposedMessages: chunk,
			isFinal:          index == len(chunks)-1,
		}

		protoErr = appendClient.Send(request.build())
		if protoErr != nil {
			err = client.grpcClient.HandleError(handle, headers, trailers, protoErr,
				BatchAppendToStream_FailedSendMessageErr)
			return BatchAppendResponse{}, err
		}
	}

	response, protoErr := appendClient.Recv()
	if protoErr != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, protoErr,
			BatchAppendToStream_FailedToCloseStreamErr)
		return BatchAppendResponse{}, err
	}

	return client.batchResponseAdapter.createResponseWithError(response)
}

// SetStreamMetadata writes stream's metadata.
// Streams metadata are a series of events, each event represented by StreamMetadata.
//
//Stream's metadata are kept in a separate stream which begins with a prefix $$.
//
// For example: for stream my_card, it's metadata stream will be $my_card.
func (client *ClientImpl) SetStreamMetadata(
	ctx context.Context,
	streamID string,
	expectedStreamRevision IsWriteStreamRevision,
	metadata StreamMetadata) (AppendResponse, errors.Error) {
	streamMetadataEvent := newMetadataEvent(metadata)

	return client.appendToStreamWithError(ctx, appendRequestContentOptions{
		streamId:               getMetaStreamOf(streamID),
		expectedStreamRevision: expectedStreamRevision,
	}, []ProposedEvent{streamMetadataEvent})
}

// GetStreamMetadata reads stream's latest metadata.
func (client *ClientImpl) GetStreamMetadata(
	ctx context.Context,
	streamId string) (StreamMetadataResult, errors.Error) {

	events, err := client.readStreamEvents(ctx, readRequest{
		streamOption: readRequestStreamOptions{
			StreamIdentifier: getMetaStreamOf(streamId),
			Revision:         ReadStreamRevisionEnd{},
		},
		direction:    ReadDirectionBackward,
		resolveLinks: false,
		count:        1,
		filter:       noFilter{},
	})
	if err != nil {
		if err.Code() == errors.StreamNotFoundErr {
			return StreamMetadataResult{
				streamId: streamId,
			}, nil
		}
		return StreamMetadataResult{
			streamId: streamId,
		}, err
	}

	if len(events) == 0 {
		return StreamMetadataResult{
			streamId: streamId,
		}, nil
	}

	return newStreamMetadataResultImpl(streamId, events[0]), nil
}

// FailedToDeleteStreamErr indicates that client's Client.DeleteStream received an unknown error
// when it tried to soft-delete an EventStoreDB stream.
const FailedToDeleteStreamErr errors.ErrorCode = "FailedToDeleteStreamErr"

// DeleteStream performs a soft delete on a stream.
//
// Appending events to soft-deleted stream with WriteStreamRevisionStreamExists will fail
// with error errors.StreamDeleted.
//
// Soft-deleted stream is a stream to which events can be appended
// using for example WriteStreamRevisionNoStream and WriteStreamRevisionAny.
//
// The only events which can be read from a soft-deleted stream are only the ones
// which were written after a soft-delete. Any events written previous to soft-delete
// are out of reach.
func (client *ClientImpl) DeleteStream(
	ctx context.Context,
	streamID string,
	revision IsWriteStreamRevision) (DeleteResponse, errors.Error) {
	return client.deleteStream(ctx, deleteRequest{
		streamId:               streamID,
		expectedStreamRevision: revision,
	})
}

// TombstoneStream performs a hard-delete on a stream.
//
// After performing a hard-delete events cannot be written or read from a stream.
func (client *ClientImpl) TombstoneStream(
	ctx context.Context,
	streamID string,
	revision IsWriteStreamRevision) (TombstoneResponse, errors.Error) {
	return client.tombstoneStream(ctx, tombstoneRequest{
		streamId:               streamID,
		expectedStreamRevision: revision,
	})
}

// FailedToTombstoneStreamErr indicates that client's Client.TombstoneStream
// received an unknown error when it tried to soft-delete an EventStoreDB stream.
const FailedToTombstoneStreamErr errors.ErrorCode = "FailedToTombstoneStreamErr"

// GetStreamReader returns a stream reader for a stream which will read events from a
// given revision towards a given direction.
//
// For example, you can read events from the end towards the start of a stream by setting
// revision to ReadStreamRevisionEnd and direction to ReadDirectionBackward.
//
// Use count to specify how many events you want to be able to read through a reader.
// Maximum number of events to read is ReadCountMax.
func (client *ClientImpl) GetStreamReader(
	ctx context.Context,
	streamID string,
	direction ReadDirection,
	revision IsReadStreamRevision,
	count uint64,
	resolveLinks bool, // Todo add documentation for resolveLinks
) (StreamReader, errors.Error) {
	return client.getStreamEventsReader(ctx, readRequest{
		streamOption: readRequestStreamOptions{
			StreamIdentifier: streamID,
			Revision:         revision,
		},
		direction:    direction,
		resolveLinks: resolveLinks,
		count:        count,
		filter:       noFilter{},
	})
}

// GetStreamReaderForStreamAll returns a reader for a stream $all which will read events
// from a given position towards a given direction.
//
// For example, you can read events from the end towards the start of a stream $all by setting
// revision to ReadPositionAllEnd and direction to ReadDirectionBackward.
//
// Use count to specify how many events you want to be able to read through a reader.
// Maximum number of events to read is ReadCountMax.
func (client *ClientImpl) GetStreamReaderForStreamAll(
	ctx context.Context,
	direction ReadDirection,
	position IsReadPositionAll,
	count uint64,
	resolveLinks bool, // Todo add documentation for resolveLinks
) (StreamReader, errors.Error) {
	return client.getStreamEventsReader(ctx, readRequest{
		streamOption: readRequestStreamOptionsAll{
			Position: position,
		},
		direction:    direction,
		resolveLinks: resolveLinks,
		count:        count,
		filter:       noFilter{},
	})
}

// SubscribeToStream subscribes to a stream in a form of a live subscription, starting from a
// given revision.
//
// Revision indicates from which point in a stream we want to receive content.
// Content can be received from the beginning of a stream, the end of a stream of
// from other specific revision.
//
// If we opt to receive content from start of the stream we will receive all content for the stream,
// eventually, unless we cancel our subscription.
//
// If we opt to receive content from the end of a stream then we will receive only
// content written to a stream after our subscription was created.
//
// If we set a specific point from which we want to start to receive content of the stream,
// then we will start to receive content only when that point (index) is reached.
// For example: If we subscribe from revision 5 and stream currently contains only one event,
// then our subscription will receive content, only after 4 events have been written to a stream.
// That means that our subscription will receive the 6th event written to a stream and all
// content written to a stream after it.
//
// If you only want to receive new content, set revision to ReadStreamRevisionEnd.
func (client *ClientImpl) SubscribeToStream(
	ctx context.Context,
	streamID string,
	revision IsReadStreamRevision,
	resolveLinks bool, // Todo add documentation for resolveLinks
) (StreamReader, errors.Error) {
	return client.subscribeToStream(ctx, subscribeToStreamRequest{
		streamOption: subscribeRequestStreamOptions{
			StreamIdentifier: streamID,
			Revision:         revision,
		},
		direction:    subscribeRequestDirectionForward,
		resolveLinks: resolveLinks,
		filter:       noFilter{},
	})
}

// SubscribeToFilteredStreamAll subscribes to stream $all using a
// filter and receives content from it.
//
// Filter is used to filter by event's type or by a stream ID.
// Both can be filtered using a set of prefixes or by a regex.
//
// Revision indicates from which point in a stream we want to receive content.
// Content can be received from the beginning of a stream, the end of a stream of
// from other specific point.
//
// If we opt to receive content from start of the stream we will receive all content for the stream,
// eventually, unless we cancel our subscription.
//
// If we opt to receive content from the end of a stream then we will receive only
// content written to a stream after our subscription was created.
//
// If we set a specific point from which we want to start to receive content of the stream,
// then we will start to receive content only when that point (index) is reached.
//
// If you only want to receive new content from stream $all, set revision to ReadPositionAllEnd.
func (client *ClientImpl) SubscribeToFilteredStreamAll(
	ctx context.Context,
	position IsReadPositionAll,
	resolveLinks bool, // Todo add documentation for resolveLinks
	filter Filter,
) (StreamReader, errors.Error) {
	return client.subscribeToStream(ctx, subscribeToStreamRequest{
		streamOption: subscribeRequestStreamOptionsAll{
			Position: position,
		},
		direction:    subscribeRequestDirectionForward,
		resolveLinks: resolveLinks,
		filter:       filter,
	})
}

// SubscribeToStreamAll subscribes to stream $all and receives content from it.
// Content is not filtered.
//
// Revision indicates from which point in a stream we want to receive content.
// Content can be received from the beginning of a stream, the end of a stream of
// from other specific position.
//
// If we opt to receive content from start of the stream we will receive all content for the stream,
// eventually, unless we cancel our subscription.
//
// If we opt to receive content from the end of a stream then we will receive only
// content written to a stream after our subscription was created.
//
// If we set a specific point from which we want to start to receive content of the stream,
// then we will start to receive content only when that point (index) is reached.
//
// If you only want to receive new content, set revision to ReadPositionAllEnd.
func (client *ClientImpl) SubscribeToStreamAll(
	ctx context.Context,
	position IsReadPositionAll,
	resolveLinks bool, // Todo add documentation for resolveLinks
) (StreamReader, errors.Error) {
	return client.subscribeToStream(ctx, subscribeToStreamRequest{
		streamOption: subscribeRequestStreamOptionsAll{
			Position: position,
		},
		direction:    subscribeRequestDirectionForward,
		resolveLinks: resolveLinks,
		filter:       noFilter{},
	})
}

const (
	FailedToCreateReaderErr                errors.ErrorCode = "FailedToCreateReaderErr"
	FailedToReceiveSubscriptionResponseErr errors.ErrorCode = "FailedToReceiveSubscriptionResponseErr"
)

// ReadStreamEvents reads events from a given stream.
//
// Read is performed by starting from a revision and reading all events towards a given direction.
//
// For example, you can read events from the end towards the start of a stream by setting
// revision to ReadStreamRevisionEnd and direction to ReadDirectionBackward.
//
// Use count to specify how many events you want to read.
// Maximum number of events read is ReadCountMax.
func (client *ClientImpl) ReadStreamEvents(
	ctx context.Context,
	streamID string,
	direction ReadDirection,
	revision IsReadStreamRevision,
	count uint64,
	resolveLinks bool, // Todo add documentation for resolveLinks
) (ResolvedEventList, errors.Error) {
	return client.readStreamEvents(ctx, readRequest{
		streamOption: readRequestStreamOptions{
			StreamIdentifier: streamID,
			Revision:         revision,
		},
		direction:    direction,
		resolveLinks: resolveLinks,
		count:        count,
		filter:       noFilter{},
	})
}

// FailedToObtainStreamReaderErr indicates that client received an unknown error when it tried to construct
// a protobuf stream reader client.
const (
	FailedToObtainStreamReaderErr errors.ErrorCode = "FailedToObtainStreamReaderErr"
)

// ReadEventsFromStreamAll reads events from stream $all.
//
// Read is performed by starting from a position and reading all events towards a given direction.
//
// For example, you can read events from the end towards the start of a stream $all by setting
// revision to ReadPositionAllEnd and direction to ReadDirectionBackward.
//
// Use count to specify how many events you want to read.
// Maximum number of events read is ReadCountMax.
func (client *ClientImpl) ReadEventsFromStreamAll(
	ctx context.Context,
	direction ReadDirection,
	position IsReadPositionAll,
	count uint64,
	resolveLinks bool, // Todo add documentation for resolveLinks
) (ResolvedEventList, errors.Error) {

	return client.readStreamEvents(ctx, readRequest{
		streamOption: readRequestStreamOptionsAll{
			Position: position,
		},
		direction:    direction,
		resolveLinks: resolveLinks,
		count:        count,
		filter:       noFilter{},
	})
}

func (client *ClientImpl) tombstoneStream(
	context context.Context,
	tombstoneRequest tombstoneRequest,
) (TombstoneResponse, errors.Error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return TombstoneResponse{}, err
	}

	grpcStreamsClient := streams2.NewStreamsClient(handle.Connection())

	var headers, trailers metadata.MD
	tombstoneResponse, protoErr := grpcStreamsClient.Tombstone(context, tombstoneRequest.build(),
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, protoErr, FailedToTombstoneStreamErr)
		return TombstoneResponse{}, err
	}

	return client.tombstoneResponseAdapter.Create(tombstoneResponse), nil
}

func (client *ClientImpl) appendToStreamWithError(
	ctx context.Context,
	options appendRequestContentOptions,
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
			AppendToStream_FailedToObtainWriterErr)
		return AppendResponse{}, err
	}

	headerRequest := appendRequest{content: options}
	protoErr = appendClient.Send(headerRequest.build())

	if protoErr != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, protoErr,
			AppendToStream_FailedSendHeaderErr)
		return AppendResponse{}, err
	}

	for _, event := range events {
		message := appendRequest{content: event.toProposedMessage()}
		protoErr = appendClient.Send(message.build())

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

	return client.appendResponseAdapter.CreateResponseWithError(response)
}

func (client *ClientImpl) readStreamEvents(
	ctx context.Context,
	readRequest readRequest) (ResolvedEventList, errors.Error) {

	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}

	grpcStreamsClient := streams2.NewStreamsClient(handle.Connection())

	var headers, trailers metadata.MD
	ctx, cancel := context.WithCancel(ctx)
	readStreamClient, protoErr := grpcStreamsClient.Read(ctx, readRequest.build(),
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		defer cancel()
		err = client.grpcClient.HandleError(handle, headers, trailers, protoErr,
			FailedToObtainStreamReaderErr)
		return nil, err
	}

	var result ResolvedEventList

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

		readResult, err := client.readResponseAdapter.create(protoReadResult)
		if err != nil {
			cancel()
			return nil, err
		}

		if _, isCheckpoint := readResult.GetCheckpoint(); isCheckpoint {
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

func (client *ClientImpl) subscribeToStream(
	ctx context.Context,
	request subscribeToStreamRequest,
) (StreamReader, errors.Error) {
	var headers, trailers metadata.MD

	ctx, cancel := context.WithCancel(ctx)
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		defer cancel()
		return nil, err
	}

	grpcStreamsClient := streams2.NewStreamsClient(handle.Connection())

	readStreamClient, protoErr := grpcStreamsClient.Read(ctx, request.build(),
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
			readClient := client.readClientFactory.create(readStreamClient, cancel)
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

func (client *ClientImpl) getStreamEventsReader(
	ctx context.Context,
	readRequest readRequest) (StreamReader, errors.Error) {

	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}

	grpcStreamsClient := streams2.NewStreamsClient(handle.Connection())

	var headers, trailers metadata.MD
	ctx, cancel := context.WithCancel(ctx)
	readStreamClient, protoErr := grpcStreamsClient.Read(ctx, readRequest.build(),
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		defer cancel()
		err = client.grpcClient.HandleError(handle, headers, trailers, protoErr,
			FailedToObtainStreamReaderErr)
		return nil, err
	}

	readClient := client.readClientFactory.create(readStreamClient, cancel)
	return readClient, nil
}

func (client *ClientImpl) deleteStream(
	context context.Context,
	deleteRequest deleteRequest,
) (DeleteResponse, errors.Error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return DeleteResponse{}, err
	}

	grpcStreamsClient := streams2.NewStreamsClient(handle.Connection())

	var headers, trailers metadata.MD
	deleteResponse, protoErr := grpcStreamsClient.Delete(context, deleteRequest.build(),
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, protoErr, FailedToDeleteStreamErr)
		return DeleteResponse{}, err
	}

	return client.deleteResponseAdapter.Create(deleteResponse), nil
}
