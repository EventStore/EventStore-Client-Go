package client_test

import (
	"context"
	"testing"

	"github.com/EventStore/EventStore-Client-Go/messages"
	"github.com/EventStore/EventStore-Client-Go/persistent"
	"github.com/stretchr/testify/require"
)

func Test_PersistentSubscription_ReadMessages_WithAck(t *testing.T) {
	containerInstance, clientInstance := initializeContainerAndClient(t)
	defer func() {
		err := clientInstance.Close()
		require.NoError(t, err)
	}()
	defer containerInstance.Close()

	streamID := "someStream"
	firstEvent := createTestEvent()
	secondEvent := createTestEvent()
	thirdEvent := createTestEvent()
	events := []messages.ProposedEvent{firstEvent, secondEvent, thirdEvent}
	pushEventsToStream(t, clientInstance, streamID, events)

	groupName := "Group 1"
	err := clientInstance.CreatePersistentSubscription(
		context.Background(),
		persistent.SubscriptionStreamConfig{
			StreamOption: persistent.StreamSettings{
				StreamName: []byte(streamID),
				Revision:   persistent.Revision_Start,
			},
			GroupName: groupName,
			Settings:  persistent.DefaultSubscriptionSettings,
		},
	)

	var bufferSize int32 = 2
	readConnectionClient, err := clientInstance.ConnectToPersistentSubscription(
		context.Background(), bufferSize, groupName, []byte(streamID))
	require.NoError(t, err)

	firstReadEvent, err := readConnectionClient.Read()
	require.NoError(t, err)
	require.NotNil(t, firstReadEvent)

	secondReadEvent, err := readConnectionClient.Read()
	require.NoError(t, err)
	require.NotNil(t, secondReadEvent)

	// since buffer size is two, after reading two outstanding messages
	// we must acknowledge a message in order to receive third one
	err = readConnectionClient.Ack(firstReadEvent.EventID)
	require.NoError(t, err)

	thirdReadEvent, err := readConnectionClient.Read()
	require.NoError(t, err)
	require.NotNil(t, thirdReadEvent)
}

func Test_PersistentSubscription_ReadMessages_WithNack(t *testing.T) {
	containerInstance, clientInstance := initializeContainerAndClient(t)
	defer func() {
		err := clientInstance.Close()
		require.NoError(t, err)
	}()
	defer containerInstance.Close()

	streamID := "someStream"
	firstEvent := createTestEvent()
	secondEvent := createTestEvent()
	thirdEvent := createTestEvent()
	events := []messages.ProposedEvent{firstEvent, secondEvent, thirdEvent}
	pushEventsToStream(t, clientInstance, streamID, events)

	groupName := "Group 1"
	err := clientInstance.CreatePersistentSubscription(
		context.Background(),
		persistent.SubscriptionStreamConfig{
			StreamOption: persistent.StreamSettings{
				StreamName: []byte(streamID),
				Revision:   persistent.Revision_Start,
			},
			GroupName: groupName,
			Settings:  persistent.DefaultSubscriptionSettings,
		},
	)

	var bufferSize int32 = 2
	readConnectionClient, err := clientInstance.ConnectToPersistentSubscription(
		context.Background(), bufferSize, groupName, []byte(streamID))
	require.NoError(t, err)

	firstReadEvent, err := readConnectionClient.Read()
	require.NoError(t, err)
	require.NotNil(t, firstReadEvent)

	secondReadEvent, err := readConnectionClient.Read()
	require.NoError(t, err)
	require.NotNil(t, secondReadEvent)

	// since buffer size is two, after reading two outstanding messages
	// we must acknowledge a message in order to receive third one
	err = readConnectionClient.Nack("test reason", persistent.Nack_Park, firstReadEvent.EventID)
	require.NoError(t, err)

	thirdReadEvent, err := readConnectionClient.Read()
	require.NoError(t, err)
	require.NotNil(t, thirdReadEvent)
}
