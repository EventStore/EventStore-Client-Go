// Header

// Package event_streams provides interaction with EventStoreDb event streams.
// Before accessing streams a grpc connection needs to be established with EventStore through
// github.com/pivonroll/EventStore-Client-Go/connection package.
package event_streams

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/pivonroll/EventStore-Client-Go/errors"
)

// Client is an interface which provides the methods for interacting with event streams.
// Stream revision has a read (IsReadStreamRevision) and write (IsWriteStreamRevision) interfaces.
// IsWriteStreamRevision interface is used for stream revision in methods which alter the state of the stream.
// IsReadStreamRevision interface is used for stream revision in  methods which read events from the stream.
// Todo add support for filtered read of events in a stream $all
type Client interface {
	// AppendToStream appends a slice of events to a stream. Events are sent to a stream one by one.
	// If appending of one event fails EventStoreDb will roll back the whole transaction.
	// If any error occurs error will be returned with appropriate code set.
	AppendToStream(
		ctx context.Context,
		streamId string,
		expectedStreamRevision IsWriteStreamRevision,
		events []ProposedEvent,
	) (AppendResponse, errors.Error)

	// BatchAppendToStream appends events to a stream in chunks.
	// If batch append of one chunk fails EventStoreDb will roll back the whole transaction.
	// If any error occurs error will be returned with appropriate code set.
	BatchAppendToStream(ctx context.Context,
		streamId string,
		expectedStreamRevision IsWriteStreamRevision,
		events ProposedEventList,
		chunkSize uint64,
		deadline time.Time,
	) (BatchAppendResponse, errors.Error)

	// BatchAppendToStreamWithCorrelationId appends events to a stream in chunks.
	// You can set correlationId to keep track of the operation if you are writing
	// events asynchronously in your application.
	// If batch append of one chunk fails EventStoreDb will roll back the whole transaction.
	// If any error occurs error will be returned with appropriate code set.
	BatchAppendToStreamWithCorrelationId(ctx context.Context,
		streamId string,
		expectedStreamRevision IsWriteStreamRevision,
		correlationId uuid.UUID,
		events ProposedEventList,
		chunkSize uint64,
		deadline time.Time,
	) (BatchAppendResponse, errors.Error)

	// SetStreamMetadata writes stream's metadata.
	// Stream's metadata are kept in a separate stream which begins with a prefix $$.
	// For example: for stream card, it's metadata stream will be $$card.
	// Streams metadata are a series of events, each event represented by StreamMetadata.
	SetStreamMetadata(
		ctx context.Context,
		streamID string,
		expectedStreamRevision IsWriteStreamRevision,
		metadata StreamMetadata,
	) (AppendResponse, errors.Error)

	// GetStreamMetadata reads stream's latest metadata.
	GetStreamMetadata(
		ctx context.Context,
		streamID string,
	) (StreamMetadataResult, errors.Error)

	// DeleteStream performs a soft delete on a stream.
	// Appending events to soft-deleted stream with WriteStreamRevisionStreamExists will fail
	// with error errors.StreamDeleted.
	// Soft-deleted stream is a stream to which events can be appended
	// using for example WriteStreamRevisionNoStream and WriteStreamRevisionAny.
	// The only events which can be read from a soft-deleted stream are only the ones
	// which were written after a soft-delete. Any events written previous to soft-delete
	// are out of reach.
	DeleteStream(
		ctx context.Context,
		streamID string,
		revision IsWriteStreamRevision,
	) (DeleteResponse, errors.Error)

	// TombstoneStream performs a hard-delete on a stream.
	// After performing a hard-delete events cannot be written or read from a stream.
	TombstoneStream(
		ctx context.Context,
		streamID string,
		revision IsWriteStreamRevision,
	) (TombstoneResponse, errors.Error)

	// ReadStreamEvents reads events from a given stream.
	// Read is performed by starting from a revision and reading all events towards a given direction.
	// For example, you can read events from the end towards the start of a stream by setting
	// revision to ReadStreamRevisionEnd and direction to ReadDirectionBackward.
	// Use count to specify how many events you want to read.
	// Maximum number of events read is ReadCountMax.
	// Todo add documentation for resolveLinks
	ReadStreamEvents(
		ctx context.Context,
		streamID string,
		direction ReadDirection,
		revision IsReadStreamRevision,
		count uint64,
		resolveLinks bool,
	) (ResolvedEventList, errors.Error)

	// ReadEventsFromStreamAll reads events from stream $all.
	// Read is performed by starting from a position and reading all events towards a given direction.
	// For example, you can read events from the end towards the start of a stream $all by setting
	// revision to ReadPositionAllEnd and direction to ReadDirectionBackward.
	// Use count to specify how many events you want to read.
	// Maximum number of events read is ReadCountMax.
	// Todo add documentation for resolveLinks
	ReadEventsFromStreamAll(
		ctx context.Context,
		direction ReadDirection,
		position IsReadPositionAll,
		count uint64,
		resolveLinks bool,
	) (ResolvedEventList, errors.Error)

	// GetStreamReader returns a stream reader for a stream which will read events from a
	// given revision towards a given direction.
	// For example, you can read events from the end towards the start of a stream by setting
	// revision to ReadStreamRevisionEnd and direction to ReadDirectionBackward.
	// Use count to specify how many events you want to be able to read through a reader.
	// Maximum number of events to read is ReadCountMax.
	// Todo add documentation for resolveLinks
	GetStreamReader(
		ctx context.Context,
		streamID string,
		direction ReadDirection,
		revision IsReadStreamRevision,
		count uint64,
		resolveLinks bool,
	) (StreamReader, errors.Error)

	// GetStreamReaderForStreamAll returns a reader for a stream $all which will read events
	// from a given position towards a given direction.
	// For example, you can read events from the end towards the start of a stream $all by setting
	// revision to ReadPositionAllEnd and direction to ReadDirectionBackward.
	// Use count to specify how many events you want to be able to read through a reader.
	// Maximum number of events to read is ReadCountMax.
	// Todo add documentation for resolveLinks
	GetStreamReaderForStreamAll(
		ctx context.Context,
		direction ReadDirection,
		position IsReadPositionAll,
		count uint64,
		resolveLinks bool,
	) (StreamReader, errors.Error)

	// SubscribeToStream subscribes to a stream in a form of a live subscription, starting from a
	// given revision.
	// Although it is a live subscription, events already present in a stream can be received
	// depending on how revision is set.
	// If you only want to receive new events, set revision to ReadStreamRevisionEnd.
	// Todo add documentation for resolveLinks
	SubscribeToStream(
		ctx context.Context,
		streamID string,
		revision IsReadStreamRevision,
		resolveLinks bool,
	) (StreamReader, errors.Error)

	// SubscribeToFilteredStreamAll subscribes to stream $all using a
	// filter and receives events from it.
	// Filter is used to filter by event's type or by a stream ID.
	// Both can be filtered using a set of prefixes or by a regex.
	// Although it is a live subscription, events already present in a stream can be received
	// depending on how position is set.
	// If you only want to receive new events, set revision to ReadPositionAllEnd.
	// Todo add documentation for resolveLinks
	SubscribeToFilteredStreamAll(
		ctx context.Context,
		position IsReadPositionAll,
		resolveLinks bool,
		filter Filter,
	) (StreamReader, errors.Error)

	// SubscribeToStreamAll subscribes to stream $all and receives events from it.
	// Events will not be filtered.
	// Although it is a live subscription, events already present in a stream can be received
	// depending on how position is set.
	// If you only want to receive new events, set revision to ReadPositionAllEnd.
	// Todo add documentation for resolveLinks
	SubscribeToStreamAll(
		ctx context.Context,
		position IsReadPositionAll,
		resolveLinks bool,
	) (StreamReader, errors.Error)
}
