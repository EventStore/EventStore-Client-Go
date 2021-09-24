package event_streams

import (
	"context"

	"github.com/pivonroll/EventStore-Client-Go/errors"
)

type Client interface {
	AppendToStream(
		ctx context.Context,
		streamID string,
		expectedStreamRevision IsAppendRequestExpectedStreamRevision,
		events []ProposedEvent,
	) (AppendResponse, errors.Error)

	SetStreamMetadata(
		ctx context.Context,
		streamID string,
		expectedStreamRevision IsAppendRequestExpectedStreamRevision,
		metadata StreamMetadata) (AppendResponse, errors.Error)

	DeleteStream(
		ctx context.Context,
		streamID string,
		revision IsDeleteRequestExpectedStreamRevision) (DeleteResponse, errors.Error)

	TombstoneStream(
		ctx context.Context,
		streamID string,
		revision IsTombstoneRequestExpectedStreamRevision) (TombstoneResponse, errors.Error)

	GetStreamMetadata(
		ctx context.Context,
		streamID string) (StreamMetadataResult, errors.Error)

	ReadStreamEvents(
		ctx context.Context,
		streamID string,
		direction ReadRequestDirection,
		revision IsReadRequestStreamOptionsStreamRevision,
		count uint64,
		resolveLinks bool) (ReadResponseEventList, errors.Error)

	ReadAllEvents(
		ctx context.Context,
		direction ReadRequestDirection,
		position IsReadRequestOptionsAllPosition,
		count uint64,
		resolveLinks bool,
	) (ReadResponseEventList, errors.Error)

	GetStreamReader(
		ctx context.Context,
		streamID string,
		direction ReadRequestDirection,
		revision IsReadRequestStreamOptionsStreamRevision,
		count uint64,
		resolveLinks bool) (StreamReader, errors.Error)

	GetAllEventsReader(
		ctx context.Context,
		direction ReadRequestDirection,
		position IsReadRequestOptionsAllPosition,
		count uint64,
		resolveLinks bool,
	) (StreamReader, errors.Error)

	SubscribeToStream(
		ctx context.Context,
		streamID string,
		revision IsSubscribeRequestStreamOptionsStreamRevision,
		resolveLinks bool,
	) (StreamReader, errors.Error)

	SubscribeToAllFiltered(
		ctx context.Context,
		position IsSubscribeRequestOptionsAllPosition,
		resolveLinks bool,
		filter SubscribeRequestFilter,
	) (StreamReader, errors.Error)
}
