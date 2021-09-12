package event_streams

import (
	"context"

	"github.com/EventStore/EventStore-Client-Go/connection"
)

type Client interface {
	GetReader(ctx context.Context,
		handle connection.ConnectionHandle,
		request ReadRequest) (ReadClient, error)
	GetAppender(ctx context.Context,
		handle connection.ConnectionHandle) (Appender, error)
	Delete(ctx context.Context,
		handle connection.ConnectionHandle,
		request DeleteRequest) (DeleteResponse, error)
	Tombstone(ctx context.Context,
		handle connection.ConnectionHandle,
		request TombstoneRequest) (TombstoneResponse, error)
	GetBatchAppender(ctx context.Context,
		handle connection.ConnectionHandle) (BatchAppender, error)
}
