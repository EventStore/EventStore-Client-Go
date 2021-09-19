package event_streams

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/EventStore/EventStore-Client-Go/connection"
	"github.com/EventStore/EventStore-Client-Go/protos/streams2"
	"github.com/davecgh/go-spew/spew"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type ClientImpl struct {
	grpcClient               connection.GrpcClient
	grpcStreamsClient        streams2.StreamsClient
	deleteResponseAdapter    deleteResponseAdapter
	tombstoneResponseAdapter tombstoneResponseAdapter
	readClientFactory        ReadClientFactory
	appendResponseAdapter    appendResponseAdapter
	readResponseAdapter      readResponseAdapter
}

const (
	AppendToStream_FailedToObtainAppenderErr = "AppendToStream_FailedToObtainAppenderErr"
	AppendToStream_FailedSendHeaderErr       = "AppendToStream_FailedSendHeaderErr"
	AppendToStream_FailedSendMessageErr      = "AppendToStream_FailedSendMessageErr"
	AppendToStream_FailedToCloseStreamErr    = "AppendToStream_FailedToCloseStreamErr"
)

func (client *ClientImpl) AppendToStream(
	ctx context.Context,
	options AppendRequestContentOptions,
	events []ProposedEvent,
) (AppendResponse, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return AppendResponse{}, err
	}

	var headers, trailers metadata.MD
	appendClient, err := client.grpcStreamsClient.Append(ctx,
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return AppendResponse{}, errors.New(AppendToStream_FailedToObtainAppenderErr)
	}

	headerRequest := AppendRequest{Content: options}
	err = appendClient.Send(headerRequest.Build())

	if err != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return AppendResponse{}, errors.New(AppendToStream_FailedSendHeaderErr)
	}

	for _, event := range events {
		message := AppendRequest{Content: event.ToProposedMessage()}
		err = appendClient.Send(message.Build())

		if err != nil {
			err = client.grpcClient.HandleError(handle, headers, trailers, err)
			return AppendResponse{}, errors.New(AppendToStream_FailedSendMessageErr)
		}
	}

	response, err := appendClient.CloseAndRecv()
	if err != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return AppendResponse{}, errors.New(AppendToStream_FailedToCloseStreamErr)
	}

	return client.appendResponseAdapter.CreateResponse(response), nil
}

const FailedToDeleteStreamErr = "FailedToDeleteStreamErr"

func (client *ClientImpl) DeleteStream(
	context context.Context,
	deleteRequest DeleteRequest,
) (DeleteResponse, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return DeleteResponse{}, err
	}

	var headers, trailers metadata.MD
	deleteResponse, err := client.grpcStreamsClient.Delete(context, deleteRequest.Build(),
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		log.Println("Failed to perform delete, details:", err)
		return DeleteResponse{}, errors.New(FailedToDeleteStreamErr)
	}

	return client.deleteResponseAdapter.Create(deleteResponse), nil
}

const FailedToTombstoneStreamErr = "FailedToTombstoneStreamErr"

func (client *ClientImpl) TombstoneStream(
	context context.Context,
	tombstoneRequest TombstoneRequest,
) (TombstoneResponse, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return TombstoneResponse{}, err
	}
	var headers, trailers metadata.MD
	tombstoneResponse, err := client.grpcStreamsClient.Tombstone(context, tombstoneRequest.Build(),
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return TombstoneResponse{}, errors.New(FailedToTombstoneStreamErr)
	}

	return client.tombstoneResponseAdapter.Create(tombstoneResponse), nil
}

const (
	FailedToReceiveResponseErr = "FailedToReceiveResponseErr"
	StreamNotFoundErr          = "StreamNotFoundErr"
	FatalError                 = "FatalError"
)

func (client *ClientImpl) ReadStreamEvents(
	ctx context.Context,
	readRequest ReadRequest) ([]ReadResponseEvent, error) {

	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}

	var headers, trailers metadata.MD
	ctx, cancel := context.WithCancel(ctx)
	readStreamClient, err := client.grpcStreamsClient.Read(ctx, readRequest.Build(),
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		defer cancel()
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("failed to construct read stream. Reason: %v", err)
	}

	var result []ReadResponseEvent

	var errorCode *string

	for {
		protoReadResult, err := readStreamClient.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			err = client.grpcClient.HandleError(handle, headers, trailers, err)
			fmt.Println("Failed to receive subscription response. Reason: ", err)
			temp := FailedToReceiveResponseErr
			errorCode = &temp
			break
		}

		readResult := client.readResponseAdapter.Create(protoReadResult)
		if _, streamIsNotFound := readResult.GetStreamNotFound(); streamIsNotFound {
			temp := StreamNotFoundErr
			errorCode = &temp
			break
		} else if _, isCheckpoint := readResult.GetCheckpoint(); isCheckpoint {
			continue
		}

		event, _ := readResult.GetEvent()
		result = append(result, event)
	}

	if errorCode == nil {
		defer cancel()
		return result, nil
	}

	defer cancel()
	return nil, errors.New(*errorCode)
}

func (client *ClientImpl) ReadStreamEventsReader(
	ctx context.Context,
	readRequest ReadRequest) (ReadClient, error) {

	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return nil, err
	}

	var headers, trailers metadata.MD
	ctx, cancel := context.WithCancel(ctx)
	readStreamClient, err := client.grpcStreamsClient.Read(ctx, readRequest.Build(),
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		defer cancel()
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return nil, fmt.Errorf("failed to construct read stream. Reason: %v", err)
	}

	streamId := "$all"

	if streamOption, ok := readRequest.StreamOption.(ReadRequestStreamOptions); ok {
		streamId = streamOption.StreamIdentifier
	}

	readClient := client.readClientFactory.Create(readStreamClient, cancel, streamId)
	return readClient, nil
}

const (
	FailedToCreateReaderErr                = "FailedToCreateReaderErr"
	FailedToReceiveSubscriptionResponseErr = "FailedToReceiveSubscriptionResponseErr"
	FailedToSubscribe_StreamNotFoundErr    = "FailedToSubscribe_StreamNotFoundErr"
	FailedToSubscribeErr                   = "FailedToSubscribeErr"
)

func (client *ClientImpl) SubscribeToStream(
	ctx context.Context,
	handle connection.ConnectionHandle,
	request SubscribeToStreamRequest,
) (ReadClient, error) {
	var headers, trailers metadata.MD
	ctx, cancel := context.WithCancel(ctx)

	fmt.Println(spew.Sdump(request.Build()))

	readStreamClient, err := client.grpcStreamsClient.Read(ctx, request.Build(),
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		defer cancel()
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		fmt.Println("Failed to construct subscription. Reason: ", err)
		return nil, errors.New(FailedToCreateReaderErr)
	}

	readResult, err := readStreamClient.Recv()
	if err != nil {
		defer cancel()
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		fmt.Println("Failed to receive subscription response. Reason: ", err)
		return nil, errors.New(FailedToReceiveSubscriptionResponseErr)
	}

	switch readResult.Content.(type) {
	case *streams2.ReadResp_Confirmation:
		{
			confirmation := readResult.GetConfirmation()
			readClient := client.readClientFactory.Create(readStreamClient, cancel, confirmation.SubscriptionId)

			return readClient, nil
		}
	case *streams2.ReadResp_StreamNotFound_:
		{
			defer cancel()
			streamNotFoundResult := readResult.Content.(*streams2.ReadResp_StreamNotFound_)

			fmt.Println("Failed to initiate subscription because the stream was not found.",
				string(streamNotFoundResult.StreamNotFound.StreamIdentifier.StreamName))
			return nil, errors.New(FailedToSubscribe_StreamNotFoundErr)
		}
	}
	defer cancel()
	return nil, errors.New(FailedToSubscribeErr)
}
