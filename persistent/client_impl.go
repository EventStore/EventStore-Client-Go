package persistent

import (
	"context"

	persistentProto "github.com/EventStore/EventStore-Client-Go/protos/persistent"
)

type clientImpl struct {
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
	bufferSize int32,
	groupName string,
	streamName []byte,
) (SyncReadConnection, error) {
	readClient, err := client.persistentSubscriptionClient.Read(ctx)
	if err != nil {
		return nil, NewError(SubscribeToStreamSync_FailedToInitPersistentSubscriptionClientErr, err)
	}

	err = readClient.Send(toPersistentReadRequest(bufferSize, groupName, streamName))
	if err != nil {
		return nil, NewError(SubscribeToStreamSync_FailedToSendStreamInitializationErr, err)
	}

	readResult, err := readClient.Recv()
	if err != nil {
		return nil, NewError(SubscribeToStreamSync_FailedToReceiveStreamInitializationErr, err)
	}
	switch readResult.Content.(type) {
	case *persistentProto.ReadResp_SubscriptionConfirmation_:
		{
			asyncConnection := client.syncReadConnectionFactory.NewSyncReadConnection(
				readClient,
				readResult.GetSubscriptionConfirmation().SubscriptionId,
				client.messageAdapterProvider.GetMessageAdapter())

			return asyncConnection, nil
		}
	}

	return nil, NewError(SubscribeToStreamSync_NoSubscriptionConfirmationErr, err)
}

func (client clientImpl) SubscribeToAllAsync(ctx context.Context) (SyncReadConnection, error) {
	panic("implement me")
}

const CreateStreamSubscription_FailedToCreatePermanentSubscriptionErr ErrorCode = "CreateStreamSubscription_FailedToCreatePermanentSubscriptionErr"

func (client clientImpl) CreateStreamSubscription(ctx context.Context, streamConfig SubscriptionStreamConfig) error {
	createSubscriptionConfig := createRequestProto(streamConfig)
	_, err := client.persistentSubscriptionClient.Create(ctx, createSubscriptionConfig)
	if err != nil {
		return NewError(CreateStreamSubscription_FailedToCreatePermanentSubscriptionErr, err)
	}

	return nil
}

const (
	CreateAllSubscription_FailedToCreatePermanentSubscriptionErr ErrorCode = "CreateStreamSubscription_FailedToCreatePermanentSubscriptionErr"
	CreateAllSubscription_MustProvideRegexOrPrefixErr            ErrorCode = "CreateAllSubscription_MustProvideRegexOrPrefixErr"
	CreateAllSubscription_CanSetOnlyRegexOrPrefixErr             ErrorCode = "CreateAllSubscription_CanSetOnlyRegexOrPrefixErr"
)

func (client clientImpl) CreateAllSubscription(ctx context.Context, allOptions SubscriptionAllOptionConfig) error {
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

	_, err = client.persistentSubscriptionClient.Create(ctx, protoConfig)
	if err != nil {
		return NewError(CreateAllSubscription_FailedToCreatePermanentSubscriptionErr, err)
	}

	return nil
}

const UpdateStreamSubscription_FailedToUpdateErr ErrorCode = "UpdateStreamSubscription_FailedToUpdateErr"

func (client clientImpl) UpdateStreamSubscription(ctx context.Context, streamConfig SubscriptionStreamConfig) error {
	updateSubscriptionConfig := updateRequestStreamProto(streamConfig)
	_, err := client.persistentSubscriptionClient.Update(ctx, updateSubscriptionConfig)
	if err != nil {
		return NewError(UpdateStreamSubscription_FailedToUpdateErr, err)
	}

	return nil
}

const UpdateAllSubscription_FailedToUpdateErr ErrorCode = "UpdateAllSubscription_FailedToUpdateErr"

func (client clientImpl) UpdateAllSubscription(ctx context.Context, allOptions SubscriptionUpdateAllOptionConfig) error {
	updateSubscriptionConfig := UpdateRequestAllOptionsProto(allOptions)

	_, err := client.persistentSubscriptionClient.Update(ctx, updateSubscriptionConfig)
	if err != nil {
		return NewError(UpdateAllSubscription_FailedToUpdateErr, err)
	}

	return nil
}

const DeleteStreamSubscription_FailedToDeleteErr ErrorCode = "DeleteStreamSubscription_FailedToDeleteErr"

func (client clientImpl) DeleteStreamSubscription(ctx context.Context, deleteOptions DeleteOptions) error {
	deleteSubscriptionOptions := deleteRequestStreamProto(deleteOptions)
	_, err := client.persistentSubscriptionClient.Delete(ctx, deleteSubscriptionOptions)
	if err != nil {
		return NewError(DeleteStreamSubscription_FailedToDeleteErr, err)
	}

	return nil
}

const DeleteAllSubscription_FailedToDeleteErr ErrorCode = "DeleteAllSubscription_FailedToDeleteErr"

func (client clientImpl) DeleteAllSubscription(ctx context.Context, groupName string) error {
	deleteSubscriptionOptions := deleteRequestAllOptionsProto(groupName)
	_, err := client.persistentSubscriptionClient.Delete(ctx, deleteSubscriptionOptions)
	if err != nil {
		return NewError(DeleteAllSubscription_FailedToDeleteErr, err)
	}

	return nil
}

func NewClient(client persistentProto.PersistentSubscriptionsClient) Client {
	return &clientImpl{
		persistentSubscriptionClient: client,
		syncReadConnectionFactory:    SyncReadConnectionFactoryImpl{},
		messageAdapterProvider:       messageAdapterProviderImpl{},
	}
}
