package event_streams

import (
	"context"

	"github.com/gofrs/uuid"

	"github.com/pivonroll/EventStore-Client-Go/errors"
)

type Client interface {
	AppendToStream(
		ctx context.Context,
		streamID string,
		expectedStreamRevision IsWriteStreamRevision,
		events []ProposedEvent,
	) (AppendResponse, errors.Error)

	BatchAppendToStream(ctx context.Context,
		batchRequestOptions BatchAppendRequestOptions,
		events ProposedEventList,
		chunkSize uint64,
	) (BatchAppendResponse, errors.Error)

	BatchAppendToStreamWithCorrelationId(ctx context.Context,
		batchRequestOptions BatchAppendRequestOptions,
		correlationId uuid.UUID,
		events ProposedEventList,
		chunkSize uint64,
	) (BatchAppendResponse, errors.Error)

	SetStreamMetadata(
		ctx context.Context,
		streamID string,
		expectedStreamRevision IsWriteStreamRevision,
		metadata StreamMetadata) (AppendResponse, errors.Error)

	DeleteStream(
		ctx context.Context,
		streamID string,
		revision IsWriteStreamRevision) (DeleteResponse, errors.Error)

	TombstoneStream(
		ctx context.Context,
		streamID string,
		revision IsWriteStreamRevision) (TombstoneResponse, errors.Error)

	GetStreamMetadata(
		ctx context.Context,
		streamID string) (StreamMetadataResult, errors.Error)

	ReadStreamEvents(
		ctx context.Context,
		streamID string,
		direction ReadRequestDirection,
		revision IsReadStreamRevision,
		count uint64,
		resolveLinks bool) (ResolvedEventList, errors.Error)

	ReadAllEvents(
		ctx context.Context,
		direction ReadRequestDirection,
		position IsReadPositionAll,
		count uint64,
		resolveLinks bool,
	) (ResolvedEventList, errors.Error)

	GetStreamReader(
		ctx context.Context,
		streamID string,
		direction ReadRequestDirection,
		revision IsReadStreamRevision,
		count uint64,
		resolveLinks bool) (StreamReader, errors.Error)

	GetAllEventsReader(
		ctx context.Context,
		direction ReadRequestDirection,
		position IsReadPositionAll,
		count uint64,
		resolveLinks bool,
	) (StreamReader, errors.Error)

	SubscribeToStream(
		ctx context.Context,
		streamID string,
		revision IsReadStreamRevision,
		resolveLinks bool,
	) (StreamReader, errors.Error)

	SubscribeToAllFiltered(
		ctx context.Context,
		position IsReadPositionAll,
		resolveLinks bool,
		filter SubscribeRequestFilter,
	) (StreamReader, errors.Error)

	SubscribeToAll(
		ctx context.Context,
		position IsReadPositionAll,
		resolveLinks bool,
	) (StreamReader, errors.Error)
}
