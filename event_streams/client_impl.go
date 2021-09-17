package event_streams

import (
	"context"
	"errors"
	"fmt"
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
}

const (
	AppendToStream_WrongExpectedVersionErr         = "AppendToStream_WrongExpectedVersionErr"
	AppendToStream_WrongExpectedVersion_20_6_0_Err = "AppendToStream_WrongExpectedVersion_20_6_0_Err"
	AppendToStream_FailedToObtainAppenderErr       = "AppendToStream_FailedToObtainAppenderErr"
	AppendToStream_FailedSendHeaderErr             = "AppendToStream_FailedSendHeaderErr"
	AppendToStream_FailedSendMessageErr            = "AppendToStream_FailedSendMessageErr"
	AppendToStream_FailedToCloseStreamErr          = "AppendToStream_FailedToCloseStreamErr"
)

func (client *ClientImpl) AppendToStream(
	ctx context.Context,
	options AppendRequestContentOptions,
	events []ProposedEvent,
) (WriteResult, error) {
	handle, err := client.grpcClient.GetConnectionHandle()
	if err != nil {
		return WriteResult{}, err
	}

	var headers, trailers metadata.MD
	appendClient, err := client.grpcStreamsClient.Append(ctx,
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return WriteResult{}, errors.New(AppendToStream_FailedToObtainAppenderErr)
	}

	headerRequest := AppendRequest{Content: options}
	err = appendClient.Send(headerRequest.Build())

	if err != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return WriteResult{}, errors.New(AppendToStream_FailedSendHeaderErr)
	}

	for _, event := range events {
		message := AppendRequest{Content: event.ToProposedMessage()}
		err = appendClient.Send(message.Build())

		if err != nil {
			err = client.grpcClient.HandleError(handle, headers, trailers, err)
			return WriteResult{}, errors.New(AppendToStream_FailedSendMessageErr)
		}
	}

	response, err := appendClient.CloseAndRecv()
	if err != nil {
		err = client.grpcClient.HandleError(handle, headers, trailers, err)
		return WriteResult{}, errors.New(AppendToStream_FailedToCloseStreamErr)
	}

	switch response.Result.(type) {
	case *streams2.AppendResp_Success_:
		successResponse := response.Result.(*streams2.AppendResp_Success_)

		var streamRevision uint64 = 1
		revision, isCurrentRevision := successResponse.Success.
			CurrentRevisionOption.(*streams2.AppendResp_Success_CurrentRevision)
		if isCurrentRevision {
			streamRevision = revision.CurrentRevision
		}

		var commitPosition uint64
		var preparePosition uint64
		if position, ok := successResponse.Success.
			PositionOption.(*streams2.AppendResp_Success_Position); ok {
			commitPosition = position.Position.CommitPosition
			preparePosition = position.Position.PreparePosition
		} else if !isCurrentRevision {
			streamRevision = 0
		}

		return WriteResult{
			CommitPosition:      commitPosition,
			PreparePosition:     preparePosition,
			NextExpectedVersion: streamRevision,
		}, nil
	case *streams2.AppendResp_WrongExpectedVersion_:
		wrongVersion := response.Result.(*streams2.AppendResp_WrongExpectedVersion_).WrongExpectedVersion
		type currentRevisionType struct {
			Revision uint64
			NoStream bool
		}
		var currentRevision *currentRevisionType

		if wrongVersion.CurrentRevisionOption_20_6_0 != nil {
			switch wrongVersion.CurrentRevisionOption_20_6_0.(type) {
			case *streams2.AppendResp_WrongExpectedVersion_CurrentRevision_20_6_0:
				revision := wrongVersion.
					CurrentRevisionOption_20_6_0.(*streams2.AppendResp_WrongExpectedVersion_CurrentRevision_20_6_0)
				currentRevision = &currentRevisionType{
					Revision: revision.CurrentRevision_20_6_0,
					NoStream: false,
				}
			case *streams2.AppendResp_WrongExpectedVersion_NoStream_20_6_0:
				currentRevision = &currentRevisionType{
					NoStream: true,
				}
			}
		} else if wrongVersion.CurrentRevisionOption != nil {
			switch wrongVersion.CurrentRevisionOption.(type) {
			case *streams2.AppendResp_WrongExpectedVersion_CurrentRevision:
				revision := wrongVersion.
					CurrentRevisionOption.(*streams2.AppendResp_WrongExpectedVersion_CurrentRevision)

				currentRevision = &currentRevisionType{
					Revision: revision.CurrentRevision,
					NoStream: false,
				}
			case *streams2.AppendResp_WrongExpectedVersion_CurrentNoStream:
				currentRevision = &currentRevisionType{
					NoStream: true,
				}
			}
		}

		if currentRevision != nil {
			if currentRevision.NoStream {
				log.Println("Wrong expected revision. Current revision no stream")
			} else {
				log.Println("Wrong expected revision. Current revision:", currentRevision.Revision)
			}
		}

		type expectedRevisionType struct {
			Revision     uint64
			IsAny        bool
			StreamExists bool
			NoStream     bool
		}

		var expectedRevision *expectedRevisionType

		if wrongVersion.ExpectedRevisionOption_20_6_0 != nil {
			switch wrongVersion.ExpectedRevisionOption_20_6_0.(type) {
			case *streams2.AppendResp_WrongExpectedVersion_ExpectedRevision_20_6_0:
				revision := wrongVersion.
					ExpectedRevisionOption_20_6_0.(*streams2.AppendResp_WrongExpectedVersion_ExpectedRevision_20_6_0)
				expectedRevision = &expectedRevisionType{
					Revision: revision.ExpectedRevision_20_6_0,
				}
			case *streams2.AppendResp_WrongExpectedVersion_Any_20_6_0:
				expectedRevision = &expectedRevisionType{
					IsAny: true,
				}
			case *streams2.AppendResp_WrongExpectedVersion_StreamExists_20_6_0:
				expectedRevision = &expectedRevisionType{
					StreamExists: true,
				}
			}
		} else if wrongVersion.ExpectedRevisionOption != nil {
			switch wrongVersion.ExpectedRevisionOption.(type) {
			case *streams2.AppendResp_WrongExpectedVersion_ExpectedRevision:
				revision := wrongVersion.
					ExpectedRevisionOption.(*streams2.AppendResp_WrongExpectedVersion_ExpectedRevision)
				expectedRevision = &expectedRevisionType{
					Revision: revision.ExpectedRevision,
				}
			case *streams2.AppendResp_WrongExpectedVersion_ExpectedAny:
				expectedRevision = &expectedRevisionType{
					IsAny: true,
				}
			case *streams2.AppendResp_WrongExpectedVersion_ExpectedStreamExists:
				expectedRevision = &expectedRevisionType{
					StreamExists: true,
				}
			case *streams2.AppendResp_WrongExpectedVersion_ExpectedNoStream:
				expectedRevision = &expectedRevisionType{
					NoStream: true,
				}
			}
		}

		if expectedRevision != nil {
			if expectedRevision.StreamExists {
				log.Println("Wrong expected revision. Stream Exists!")
			} else if expectedRevision.IsAny {
				log.Println("Wrong expected revision. Any!")
			} else if expectedRevision.NoStream {
				log.Println("Wrong expected revision. No Stream!")
			} else {
				log.Println("Wrong expected revision. Expected revision: ", expectedRevision.Revision)
			}
		}

		if wrongVersion.CurrentRevisionOption_20_6_0 != nil ||
			wrongVersion.ExpectedRevisionOption_20_6_0 != nil {
			return WriteResult{}, errors.New(AppendToStream_WrongExpectedVersion_20_6_0_Err)
		}
		return WriteResult{}, errors.New(AppendToStream_WrongExpectedVersionErr)
	}

	return WriteResult{
		CommitPosition:      0,
		PreparePosition:     0,
		NextExpectedVersion: 1,
	}, nil
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

func (client *ClientImpl) ReadStreamEvents(
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
