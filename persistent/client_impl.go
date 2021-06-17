package persistent

import (
	"context"
	"fmt"

	persistentProto "github.com/EventStore/EventStore-Client-Go/protos/persistent"
)

type clientImpl struct {
	persistentSubscriptionClient persistentProto.PersistentSubscriptionsClient
}

func (client clientImpl) SubscribeToStreamAsync(
	ctx context.Context,
	bufferSize int32,
	groupName string,
	streamName []byte,
	eventAppeared EventAppearedHandler,
	subscriptionDropped SubscriptionDroppedHandler,
) (Connection, error) {
	readClient, err := client.persistentSubscriptionClient.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to init persisitent subscription client. Reason %v", err)
	}

	err = readClient.Send(ToPersistentReadRequest(bufferSize, groupName, streamName))
	if err != nil {
		return nil, fmt.Errorf("failed to send connection details for persisitent subscription. Reason %v", err)
	}

	readResult, err := readClient.Recv()
	if err != nil {
		return nil, fmt.Errorf("failed to read from persisitent subscription. Reason %v", err)
	}
	switch readResult.Content.(type) {
	case *persistentProto.ReadResp_SubscriptionConfirmation_:
		{
			return newSubscriptionConnection(
				readClient,
				readResult.GetSubscriptionConfirmation().SubscriptionId,
				eventAppeared,
				subscriptionDropped), nil
		}
	}

	return nil, fmt.Errorf("failed to connect to persistent subscription")
}

func (client clientImpl) SubscribeToAllAsync(ctx context.Context) (Connection, error) {
	panic("implement me")
}

func (client clientImpl) CreateStreamSubscription(ctx context.Context, streamConfig SubscriptionStreamConfig) error {
	createSubscriptionConfig := CreateRequestProto(streamConfig)
	_, err := client.persistentSubscriptionClient.Create(ctx, createSubscriptionConfig)
	if err != nil {
		return fmt.Errorf("failed to create permanent subscription. Error: %v", err)
	}

	return nil
}

func (client clientImpl) CreateAllSubscription(ctx context.Context, allOptions SubscriptionAllOptionConfig) error {
	protoConfig, err := CreateRequestAllOptionsProto(allOptions)
	if err != nil {
		return err
	}

	_, err = client.persistentSubscriptionClient.Create(ctx, protoConfig)
	if err != nil {
		return fmt.Errorf("failed to create permanent subscription all. Error: %v", err)
	}

	return nil
}

func (client clientImpl) UpdateStreamSubscription(ctx context.Context, streamConfig SubscriptionStreamConfig) error {
	updateSubscriptionConfig := UpdateRequestStreamProto(streamConfig)
	_, err := client.persistentSubscriptionClient.Update(ctx, updateSubscriptionConfig)
	if err != nil {
		return fmt.Errorf("failed to update permanent subscription. Error: %v", err)
	}

	return nil
}

func (client clientImpl) UpdateAllSubscription(ctx context.Context, allOptions SubscriptionUpdateAllOptionConfig) error {
	updateSubscriptionConfig, err := UpdateRequestAllOptionsProto(allOptions)
	if err != nil {
		return err
	}
	_, err = client.persistentSubscriptionClient.Update(ctx, updateSubscriptionConfig)
	if err != nil {
		return fmt.Errorf("failed to update permanent subscription all. Error: %v", err)
	}

	return nil
}

func (client clientImpl) DeleteStreamSubscription(ctx context.Context, deleteOptions DeleteOptions) error {
	deleteSubscriptionOptions := DeleteRequestStreamProto(deleteOptions)
	_, err := client.persistentSubscriptionClient.Delete(ctx, deleteSubscriptionOptions)
	if err != nil {
		return fmt.Errorf("failed to delete permanent subscription. Error: %v", err)
	}

	return nil
}

func (client clientImpl) DeleteAllSubscription(ctx context.Context, groupName string) error {
	deleteSubscriptionOptions := DeleteRequestAllOptionsProto(groupName)
	_, err := client.persistentSubscriptionClient.Delete(ctx, deleteSubscriptionOptions)
	if err != nil {
		return fmt.Errorf("failed to delete permanent subscription all. Error: %v", err)
	}

	return nil
}

func NewClient(client persistentProto.PersistentSubscriptionsClient) Client {
	return &clientImpl{
		persistentSubscriptionClient: client,
	}
}
