package event_streams

import (
	"context"
	"errors"

	"github.com/EventStore/EventStore-Client-Go/connection"
	"github.com/EventStore/EventStore-Client-Go/protos/streams2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type ClientImpl struct {
	grpcClient               connection.GrpcClient
	grpcStreamsClient        streams2.StreamsClient
	readClientFactory        ReadClientFactory
	appendClientFactory      AppendClientFactory
	batchAppendClientFactory BatchAppendClientFactory
	deleteResponseAdapter    deleteResponseAdapter
	tombstoneResponseAdapter tombstoneResponseAdapter
}

const FailedToCreateReadClientErr = "FailedToCreateReadClientErr"

func (this *ClientImpl) GetReader(
	ctx context.Context,
	handle connection.ConnectionHandle,
	request ReadRequest) (ReadClient, error) {
	var headers, trailers metadata.MD

	readClient, err := this.grpcStreamsClient.Read(ctx, request.Build(),
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = this.grpcClient.HandleError(handle, headers, trailers, err)
		return nil, errors.New(FailedToCreateReadClientErr)
	}

	result := this.readClientFactory.Create(readClient)

	return result, nil
}

func (this *ClientImpl) GetAppender(
	ctx context.Context,
	handle connection.ConnectionHandle) (Appender, error) {
	var headers, trailers metadata.MD

	appendClient, err := this.grpcStreamsClient.Append(ctx,
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = this.grpcClient.HandleError(handle, headers, trailers, err)
		return nil, errors.New(FailedToCreateReadClientErr)
	}

	result := this.appendClientFactory.Create(appendClient)

	return result, nil
}

const FailedToDeleteStreamErr = "FailedToDeleteStreamErr"

func (this *ClientImpl) Delete(
	ctx context.Context,
	handle connection.ConnectionHandle,
	request DeleteRequest) (DeleteResponse, error) {
	var headers, trailers metadata.MD
	deleteResponse, err := this.grpcStreamsClient.Delete(ctx, request.Build(),
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = this.grpcClient.HandleError(handle, headers, trailers, err)
		return DeleteResponse{}, errors.New(FailedToDeleteStreamErr)
	}

	result := this.deleteResponseAdapter.Create(deleteResponse)

	return result, nil
}

func (this *ClientImpl) Tombstone(
	ctx context.Context,
	handle connection.ConnectionHandle,
	request TombstoneRequest) (TombstoneResponse, error) {
	var headers, trailers metadata.MD
	deleteResponse, err := this.grpcStreamsClient.Tombstone(ctx, request.Build(),
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = this.grpcClient.HandleError(handle, headers, trailers, err)
		return TombstoneResponse{}, errors.New(FailedToDeleteStreamErr)
	}

	result := this.tombstoneResponseAdapter.Create(deleteResponse)

	return result, nil
}

func (this *ClientImpl) GetBatchAppender(
	ctx context.Context,
	handle connection.ConnectionHandle) (BatchAppender, error) {
	var headers, trailers metadata.MD

	batchAppendClient, err := this.grpcStreamsClient.BatchAppend(ctx,
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = this.grpcClient.HandleError(handle, headers, trailers, err)
		return nil, errors.New(FailedToCreateReadClientErr)
	}

	result := this.batchAppendClientFactory.Create(batchAppendClient)

	return result, nil
}
