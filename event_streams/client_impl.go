package event_streams

import (
	"context"
	"fmt"
	"io"

	"github.com/EventStore/EventStore-Client-Go/connection"
	"github.com/EventStore/EventStore-Client-Go/errors"
	"github.com/EventStore/EventStore-Client-Go/protos/streams2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type ClientImpl struct {
	grpcClient               connection.GrpcClient
	deleteResponseAdapter    deleteResponseAdapter
	tombstoneResponseAdapter tombstoneResponseAdapter
	readClientFactory        ReadClientFactory
	appendResponseAdapter    appendResponseAdapter
	readResponseAdapter      readResponseAdapter
}

const (
	AppendToStream_FailedToObtainAppenderErr errors.ErrorCode = "AppendToStream_FailedToObtainAppenderErr"
	AppendToStream_FailedSendHeaderErr       errors.ErrorCode = "AppendToStream_FailedSendHeaderErr"
	AppendToStream_FailedSendMessageErr      errors.ErrorCode = "AppendToStream_FailedSendMessageErr"
	AppendToStream_FailedToCloseStreamErr    errors.ErrorCode = "AppendToStream_FailedToCloseStreamErr"
)

func (client *ClientImpl) AppendToStream(
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

const FailedToDeleteStreamErr errors.ErrorCode = "FailedToDeleteStreamErr"

func (client *ClientImpl) DeleteStream(
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

const FailedToTombstoneStreamErr errors.ErrorCode = "FailedToTombstoneStreamErr"

func (client *ClientImpl) TombstoneStream(
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

func (client *ClientImpl) ReadStreamEvents(
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
			fmt.Println("Failed to receive subscription response. Reason: ", err)
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

func (client *ClientImpl) ReadStreamEventsReader(
	ctx context.Context,
	readRequest ReadRequest) (ReadClient, errors.Error) {

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

const (
	FailedToCreateReaderErr                errors.ErrorCode = "FailedToCreateReaderErr"
	FailedToReceiveSubscriptionResponseErr errors.ErrorCode = "FailedToReceiveSubscriptionResponseErr"
	FailedToSubscribe_StreamNotFoundErr    errors.ErrorCode = "FailedToSubscribe_StreamNotFoundErr"
)

func (client *ClientImpl) SubscribeToStream(
	ctx context.Context,
	request SubscribeToStreamRequest,
) (ReadClient, errors.Error) {
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
			streamNotFoundResult := readResult.Content.(*streams2.ReadResp_StreamNotFound_)

			fmt.Println("Failed to initiate subscription because the stream was not found.",
				string(streamNotFoundResult.StreamNotFound.StreamIdentifier.StreamName))
			return nil, errors.NewErrorCode(FailedToSubscribe_StreamNotFoundErr)
		}
	}
	defer cancel()
	return nil, errors.NewErrorCode(errors.FatalError)
}
