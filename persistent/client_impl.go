package persistent

import (
	"context"

	"github.com/EventStore/EventStore-Client-Go/connection"
	"github.com/EventStore/EventStore-Client-Go/errors"
	persistentProto "github.com/EventStore/EventStore-Client-Go/protos/persistent"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type clientImpl struct {
	grpcClient                   connection.GrpcClient
	persistentSubscriptionClient persistentProto.PersistentSubscriptionsClient
	syncReadConnectionFactory    SyncReadConnectionFactory
	messageAdapterProvider       messageAdapterProvider
}

const (
	SubscribeToStreamSync_FailedToInitPersistentSubscriptionClientErr errors.ErrorCode = "SubscribeToStreamSync_FailedToInitPersistentSubscriptionClientErr"
	SubscribeToStreamSync_FailedToSendStreamInitializationErr         errors.ErrorCode = "SubscribeToStreamSync_FailedToSendStreamInitializationErr"
	SubscribeToStreamSync_FailedToReceiveStreamInitializationErr      errors.ErrorCode = "SubscribeToStreamSync_FailedToReceiveStreamInitializationErr"
	SubscribeToStreamSync_NoSubscriptionConfirmationErr               errors.ErrorCode = "SubscribeToStreamSync_NoSubscriptionConfirmationErr"
)

func (client clientImpl) SubscribeToStreamSync(
	ctx context.Context,
	handle connection.ConnectionHandle,
	bufferSize int32,
	groupName string,
	streamName string,
) (SyncReadConnection, errors.Error) {
	var headers, trailers metadata.MD
	ctx, cancel := context.WithCancel(ctx)
	readClient, protoErr := client.persistentSubscriptionClient.Read(ctx, grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		defer cancel()
		err := client.grpcClient.HandleError(handle, headers, trailers, protoErr,
			SubscribeToStreamSync_FailedToInitPersistentSubscriptionClientErr)
		return nil, err
	}

	protoErr = readClient.Send(toPersistentReadRequest(bufferSize, groupName, streamName))
	if protoErr != nil {
		defer cancel()
		return nil, errors.NewError(SubscribeToStreamSync_FailedToSendStreamInitializationErr, protoErr)
	}

	readResult, err := readClient.Recv()
	if err != nil {
		defer cancel()
		return nil, errors.NewError(SubscribeToStreamSync_FailedToReceiveStreamInitializationErr, err)
	}
	switch readResult.Content.(type) {
	case *persistentProto.ReadResp_SubscriptionConfirmation_:
		{
			asyncConnection := client.syncReadConnectionFactory.NewSyncReadConnection(
				readClient,
				readResult.GetSubscriptionConfirmation().SubscriptionId,
				client.messageAdapterProvider.GetMessageAdapter(),
				cancel)

			return asyncConnection, nil
		}
	}

	defer cancel()
	return nil, errors.NewError(SubscribeToStreamSync_NoSubscriptionConfirmationErr, err)
}

const CreateStreamSubscription_FailedToCreatePermanentSubscriptionErr errors.ErrorCode = "CreateStreamSubscription_FailedToCreatePermanentSubscriptionErr"

func (client clientImpl) CreateStreamSubscription(
	ctx context.Context,
	handle connection.ConnectionHandle,
	request CreateOrUpdateStreamRequest) errors.Error {
	var headers, trailers metadata.MD
	_, protoErr := client.persistentSubscriptionClient.Create(ctx, request.BuildCreateStreamRequest(),
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		err := client.grpcClient.HandleError(handle, headers, trailers, protoErr,
			CreateStreamSubscription_FailedToCreatePermanentSubscriptionErr)
		return err
	}

	return nil
}

const (
	CreateAllSubscription_FailedToCreatePermanentSubscriptionErr errors.ErrorCode = "CreateStreamSubscription_FailedToCreatePermanentSubscriptionErr"
)

func (client clientImpl) CreateAllSubscription(
	ctx context.Context,
	handle connection.ConnectionHandle,
	request CreateAllRequest) errors.Error {
	var headers, trailers metadata.MD
	_, protoErr := client.persistentSubscriptionClient.Create(ctx, request.Build(),
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		err := client.grpcClient.HandleError(handle, headers, trailers, protoErr,
			CreateAllSubscription_FailedToCreatePermanentSubscriptionErr)
		return err
	}

	return nil
}

const UpdateStreamSubscription_FailedToUpdateErr errors.ErrorCode = "UpdateStreamSubscription_FailedToUpdateErr"

func (client clientImpl) UpdateStreamSubscription(
	ctx context.Context,
	handle connection.ConnectionHandle,
	request CreateOrUpdateStreamRequest) errors.Error {
	var headers, trailers metadata.MD
	_, protoErr := client.persistentSubscriptionClient.Update(ctx, request.BuildUpdateStreamRequest(),
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		err := client.grpcClient.HandleError(handle, headers, trailers, protoErr,
			UpdateStreamSubscription_FailedToUpdateErr)
		return err
	}

	return nil
}

const UpdateAllSubscription_FailedToUpdateErr errors.ErrorCode = "UpdateAllSubscription_FailedToUpdateErr"

func (client clientImpl) UpdateAllSubscription(
	ctx context.Context,
	handle connection.ConnectionHandle,
	request UpdateAllRequest) errors.Error {
	var headers, trailers metadata.MD
	_, protoErr := client.persistentSubscriptionClient.Update(ctx, request.Build(),
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		err := client.grpcClient.HandleError(handle, headers, trailers, protoErr,
			UpdateAllSubscription_FailedToUpdateErr)
		return err
	}

	return nil
}

const DeleteStreamSubscription_FailedToDeleteErr errors.ErrorCode = "DeleteStreamSubscription_FailedToDeleteErr"

func (client clientImpl) DeleteStreamSubscription(
	ctx context.Context,
	handle connection.ConnectionHandle,
	request DeleteRequest) errors.Error {
	var headers, trailers metadata.MD
	_, protoErr := client.persistentSubscriptionClient.Delete(ctx, request.Build(),
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		err := client.grpcClient.HandleError(handle, headers, trailers, protoErr,
			DeleteStreamSubscription_FailedToDeleteErr)
		return err
	}

	return nil
}

const DeleteAllSubscription_FailedToDeleteErr errors.ErrorCode = "DeleteAllSubscription_FailedToDeleteErr"

func (client clientImpl) DeleteAllSubscription(
	ctx context.Context,
	handle connection.ConnectionHandle,
	groupName string) errors.Error {
	protoRequest := deleteRequestAllOptionsProto(groupName)
	var headers, trailers metadata.MD
	_, protoErr := client.persistentSubscriptionClient.Delete(ctx, protoRequest,
		grpc.Header(&headers), grpc.Trailer(&trailers))
	if protoErr != nil {
		err := client.grpcClient.HandleError(handle, headers, trailers, protoErr,
			DeleteAllSubscription_FailedToDeleteErr)
		return err
	}

	return nil
}

func newClientImpl(grpcClient connection.GrpcClient, client persistentProto.PersistentSubscriptionsClient) clientImpl {
	return clientImpl{
		grpcClient:                   grpcClient,
		persistentSubscriptionClient: client,
		syncReadConnectionFactory:    SyncReadConnectionFactoryImpl{},
		messageAdapterProvider:       messageAdapterProviderImpl{},
	}
}
