package persistent

import (
	"context"

	"github.com/EventStore/EventStore-Client-Go/connection"
	persistentProto "github.com/EventStore/EventStore-Client-Go/protos/persistent"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type clientImpl struct {
	inner                        connection.GrpcClient
	persistentSubscriptionClient persistentProto.PersistentSubscriptionsClient
	syncReadConnectionFactory    SyncReadConnectionFactory
	messageAdapterProvider       messageAdapterProvider
}

const (
	SubscribeToStreamSync_FailedToInitPersistentSubscriptionClientErr ErrorCode = "SubscribeToStreamSync_FailedToInitPersistentSubscriptionClientErr"
	SubscribeToStreamSync_FailedToSendStreamInitializationErr         ErrorCode = "SubscribeToStreamSync_FailedToSendStreamInitializationErr"
	SubscribeToStreamSync_FailedToReceiveStreamInitializationErr      ErrorCode = "SubscribeToStreamSync_FailedToReceiveStreamInitializationErr"
	SubscribeToStreamSync_NoSubscriptionConfirmationErr               ErrorCode = "SubscribeToStreamSync_NoSubscriptionConfirmationErr"
)

func (client clientImpl) SubscribeToStreamSync(
	ctx context.Context,
	handle connection.ConnectionHandle,
	bufferSize int32,
	groupName string,
	streamName []byte,
) (SyncReadConnection, error) {
	var headers, trailers metadata.MD
	ctx, cancel := context.WithCancel(ctx)
	readClient, err := client.persistentSubscriptionClient.Read(ctx, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		defer cancel()
		err = client.inner.HandleError(handle, headers, trailers, err)
		return nil, NewError(SubscribeToStreamSync_FailedToInitPersistentSubscriptionClientErr, err)
	}

	err = readClient.Send(toPersistentReadRequest(bufferSize, groupName, streamName))
	if err != nil {
		defer cancel()
		return nil, NewError(SubscribeToStreamSync_FailedToSendStreamInitializationErr, err)
	}

	readResult, err := readClient.Recv()
	if err != nil {
		defer cancel()
		return nil, NewError(SubscribeToStreamSync_FailedToReceiveStreamInitializationErr, err)
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
	return nil, NewError(SubscribeToStreamSync_NoSubscriptionConfirmationErr, err)
}

const CreateStreamSubscription_FailedToCreatePermanentSubscriptionErr ErrorCode = "CreateStreamSubscription_FailedToCreatePermanentSubscriptionErr"

func (client clientImpl) CreateStreamSubscription(ctx context.Context, handle connection.ConnectionHandle, streamConfig SubscriptionStreamConfig) error {
	createSubscriptionConfig := createRequestProto(streamConfig)
	var headers, trailers metadata.MD
	_, err := client.persistentSubscriptionClient.Create(ctx, createSubscriptionConfig, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.inner.HandleError(handle, headers, trailers, err)
		return NewError(CreateStreamSubscription_FailedToCreatePermanentSubscriptionErr, err)
	}

	return nil
}

const (
	CreateAllSubscription_FailedToCreatePermanentSubscriptionErr ErrorCode = "CreateStreamSubscription_FailedToCreatePermanentSubscriptionErr"
	CreateAllSubscription_MustProvideRegexOrPrefixErr            ErrorCode = "CreateAllSubscription_MustProvideRegexOrPrefixErr"
	CreateAllSubscription_CanSetOnlyRegexOrPrefixErr             ErrorCode = "CreateAllSubscription_CanSetOnlyRegexOrPrefixErr"
)

func (client clientImpl) CreateAllSubscription(ctx context.Context, handle connection.ConnectionHandle, allOptions SubscriptionAllOptionConfig) error {
	protoConfig, err := createRequestAllOptionsProto(allOptions)
	if err != nil {
		errorCode, ok := err.(Error)

		if ok {
			if errorCode.Code() == createRequestFilterOptionsProto_MustProvideRegexOrPrefixErr {
				return NewErrorCode(CreateAllSubscription_MustProvideRegexOrPrefixErr)
			} else if errorCode.Code() == createRequestFilterOptionsProto_CanSetOnlyRegexOrPrefixErr {
				return NewErrorCode(CreateAllSubscription_CanSetOnlyRegexOrPrefixErr)
			}
		}
		return err
	}

	var headers, trailers metadata.MD
	_, err = client.persistentSubscriptionClient.Create(ctx, protoConfig, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.inner.HandleError(handle, headers, trailers, err)
		return NewError(CreateAllSubscription_FailedToCreatePermanentSubscriptionErr, err)
	}

	return nil
}

const UpdateStreamSubscription_FailedToUpdateErr ErrorCode = "UpdateStreamSubscription_FailedToUpdateErr"

func (client clientImpl) UpdateStreamSubscription(ctx context.Context, handle connection.ConnectionHandle, streamConfig SubscriptionStreamConfig) error {
	updateSubscriptionConfig := updateRequestStreamProto(streamConfig)
	var headers, trailers metadata.MD
	_, err := client.persistentSubscriptionClient.Update(ctx, updateSubscriptionConfig, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.inner.HandleError(handle, headers, trailers, err)
		return NewError(UpdateStreamSubscription_FailedToUpdateErr, err)
	}

	return nil
}

const UpdateAllSubscription_FailedToUpdateErr ErrorCode = "UpdateAllSubscription_FailedToUpdateErr"

func (client clientImpl) UpdateAllSubscription(ctx context.Context, handle connection.ConnectionHandle, allOptions SubscriptionUpdateAllOptionConfig) error {
	updateSubscriptionConfig := UpdateRequestAllOptionsProto(allOptions)

	var headers, trailers metadata.MD
	_, err := client.persistentSubscriptionClient.Update(ctx, updateSubscriptionConfig, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.inner.HandleError(handle, headers, trailers, err)
		return NewError(UpdateAllSubscription_FailedToUpdateErr, err)
	}

	return nil
}

const DeleteStreamSubscription_FailedToDeleteErr ErrorCode = "DeleteStreamSubscription_FailedToDeleteErr"

func (client clientImpl) DeleteStreamSubscription(ctx context.Context, handle connection.ConnectionHandle, deleteOptions DeleteOptions) error {
	deleteSubscriptionOptions := deleteRequestStreamProto(deleteOptions)
	var headers, trailers metadata.MD
	_, err := client.persistentSubscriptionClient.Delete(ctx, deleteSubscriptionOptions, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.inner.HandleError(handle, headers, trailers, err)
		return NewError(DeleteStreamSubscription_FailedToDeleteErr, err)
	}

	return nil
}

const DeleteAllSubscription_FailedToDeleteErr ErrorCode = "DeleteAllSubscription_FailedToDeleteErr"

func (client clientImpl) DeleteAllSubscription(ctx context.Context, handle connection.ConnectionHandle, groupName string) error {
	deleteSubscriptionOptions := deleteRequestAllOptionsProto(groupName)
	var headers, trailers metadata.MD
	_, err := client.persistentSubscriptionClient.Delete(ctx, deleteSubscriptionOptions, grpc.Header(&headers), grpc.Trailer(&trailers))
	if err != nil {
		err = client.inner.HandleError(handle, headers, trailers, err)
		return NewError(DeleteAllSubscription_FailedToDeleteErr, err)
	}

	return nil
}

func NewClient(inner connection.GrpcClient, client persistentProto.PersistentSubscriptionsClient) Client {
	return clientImpl{
		inner:                        inner,
		persistentSubscriptionClient: client,
		syncReadConnectionFactory:    SyncReadConnectionFactoryImpl{},
		messageAdapterProvider:       messageAdapterProviderImpl{},
	}
}
