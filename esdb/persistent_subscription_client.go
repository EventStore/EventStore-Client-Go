package esdb

import (
	"context"

	"github.com/EventStore/EventStore-Client-Go/protos/persistent"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type persistentClient struct {
	inner                        *grpcClient
	persistentSubscriptionClient persistent.PersistentSubscriptionsClient
}

func (client *persistentClient) ConnectToPersistentSubscription(
	ctx context.Context,
	handle connectionHandle,
	bufferSize int32,
	streamName string,
	groupName string,
	auth *Credentials,
) (*PersistentSubscription, error) {
	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	if auth != nil {
		callOptions = append(callOptions, grpc.PerRPCCredentials(basicAuth{
			username: auth.Login,
			password: auth.Password,
		}))
	}
	ctx, cancel := context.WithCancel(ctx)
	readClient, err := client.persistentSubscriptionClient.Read(ctx, callOptions...)
	if err != nil {
		defer cancel()
		err = client.inner.handleError(handle, headers, trailers, err)
		return nil, PersistentSubscriptionFailedToInitClientError(err)
	}

	err = readClient.Send(toPersistentReadRequest(bufferSize, groupName, []byte(streamName)))
	if err != nil {
		defer cancel()
		return nil, PersistentSubscriptionFailedSendStreamInitError(err)
	}

	readResult, err := readClient.Recv()
	if err != nil {
		defer cancel()
		return nil, PersistentSubscriptionFailedReceiveStreamInitError(err)
	}
	switch readResult.Content.(type) {
	case *persistent.ReadResp_SubscriptionConfirmation_:
		{
			asyncConnection := NewPersistentSubscription(
				readClient,
				readResult.GetSubscriptionConfirmation().SubscriptionId,
				cancel)

			return asyncConnection, nil
		}
	}

	defer cancel()
	return nil, PersistentSubscriptionNoConfirmationError(err)
}

func (client *persistentClient) CreateStreamSubscription(
	ctx context.Context,
	handle connectionHandle,
	streamName string,
	groupName string,
	position StreamPosition,
	settings SubscriptionSettings,
	auth *Credentials,
) error {
	createSubscriptionConfig := createPersistentRequestProto(streamName, groupName, position, settings)
	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	if auth != nil {
		callOptions = append(callOptions, grpc.PerRPCCredentials(basicAuth{
			username: auth.Login,
			password: auth.Password,
		}))
	}
	_, err := client.persistentSubscriptionClient.Create(ctx, createSubscriptionConfig, callOptions...)
	if err != nil {
		err = client.inner.handleError(handle, headers, trailers, err)
		return PersistentSubscriptionFailedCreationError(err)
	}

	return nil
}

func (client *persistentClient) CreateAllSubscription(
	ctx context.Context,
	handle connectionHandle,
	groupName string,
	position AllPosition,
	settings SubscriptionSettings,
	filter *SubscriptionFilterOptions,
	auth *Credentials,
) error {
	protoConfig, err := createPersistentRequestAllOptionsProto(groupName, position, settings, filter)
	if err != nil {
		return err
	}

	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	if auth != nil {
		callOptions = append(callOptions, grpc.PerRPCCredentials(basicAuth{
			username: auth.Login,
			password: auth.Password,
		}))
	}
	_, err = client.persistentSubscriptionClient.Create(ctx, protoConfig, callOptions...)
	if err != nil {
		err = client.inner.handleError(handle, headers, trailers, err)
		return PersistentSubscriptionFailedCreationError(err)
	}

	return nil
}

func (client *persistentClient) UpdateStreamSubscription(
	ctx context.Context,
	handle connectionHandle,
	streamName string,
	groupName string,
	position StreamPosition,
	settings SubscriptionSettings,
	auth *Credentials,
) error {
	updateSubscriptionConfig := updatePersistentRequestStreamProto(streamName, groupName, position, settings)
	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	if auth != nil {
		callOptions = append(callOptions, grpc.PerRPCCredentials(basicAuth{
			username: auth.Login,
			password: auth.Password,
		}))
	}
	_, err := client.persistentSubscriptionClient.Update(ctx, updateSubscriptionConfig, callOptions...)
	if err != nil {
		err = client.inner.handleError(handle, headers, trailers, err)
		return PersistentSubscriptionUpdateFailedError(err)
	}

	return nil
}

func (client *persistentClient) UpdateAllSubscription(
	ctx context.Context,
	handle connectionHandle,
	groupName string,
	position AllPosition,
	settings SubscriptionSettings,
	auth *Credentials,
) error {
	updateSubscriptionConfig := updatePersistentRequestAllOptionsProto(groupName, position, settings)

	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	if auth != nil {
		callOptions = append(callOptions, grpc.PerRPCCredentials(basicAuth{
			username: auth.Login,
			password: auth.Password,
		}))
	}
	_, err := client.persistentSubscriptionClient.Update(ctx, updateSubscriptionConfig, callOptions...)
	if err != nil {
		err = client.inner.handleError(handle, headers, trailers, err)
		return PersistentSubscriptionUpdateFailedError(err)
	}

	return nil
}

func (client *persistentClient) DeleteStreamSubscription(
	ctx context.Context,
	handle connectionHandle,
	streamName string,
	groupName string,
	auth *Credentials,
) error {
	deleteSubscriptionOptions := deletePersistentRequestStreamProto(streamName, groupName)
	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	if auth != nil {
		callOptions = append(callOptions, grpc.PerRPCCredentials(basicAuth{
			username: auth.Login,
			password: auth.Password,
		}))
	}
	_, err := client.persistentSubscriptionClient.Delete(ctx, deleteSubscriptionOptions, callOptions...)
	if err != nil {
		err = client.inner.handleError(handle, headers, trailers, err)
		return PersistentSubscriptionDeletionFailedError(err)
	}

	return nil
}

func (client *persistentClient) DeleteAllSubscription(ctx context.Context, handle connectionHandle, groupName string, auth *Credentials) error {
	deleteSubscriptionOptions := deletePersistentRequestAllOptionsProto(groupName)
	var headers, trailers metadata.MD
	callOptions := []grpc.CallOption{grpc.Header(&headers), grpc.Trailer(&trailers)}
	if auth != nil {
		callOptions = append(callOptions, grpc.PerRPCCredentials(basicAuth{
			username: auth.Login,
			password: auth.Password,
		}))
	}
	_, err := client.persistentSubscriptionClient.Delete(ctx, deleteSubscriptionOptions, callOptions...)
	if err != nil {
		err = client.inner.handleError(handle, headers, trailers, err)
		return PersistentSubscriptionDeletionFailedError(err)
	}

	return nil
}

func newPersistentClient(inner *grpcClient, client persistent.PersistentSubscriptionsClient) persistentClient {
	return persistentClient{
		inner:                        inner,
		persistentSubscriptionClient: client,
	}
}
