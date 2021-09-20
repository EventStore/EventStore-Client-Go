package client_test

import (
	"context"
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/event_streams"
	"github.com/EventStore/EventStore-Client-Go/persistent"
	"github.com/stretchr/testify/require"
)

func Test_PersistentSubscription_ReadExistingStream_AckToReceiveNewEvents(t *testing.T) {
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClient(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	streamID := "someStream"
	firstEvent := createTestEvent()
	secondEvent := createTestEvent()
	thirdEvent := createTestEvent()
	pushEventsToStream(t, clientInstance, streamID, firstEvent, secondEvent, thirdEvent)

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
	require.NoError(t, err)

	var bufferSize int32 = 2
	readConnectionClient, err := clientInstance.ConnectToPersistentSubscription(
		context.Background(), bufferSize, groupName, []byte(streamID))
	require.NoError(t, err)

	firstReadEvent := readConnectionClient.Recv().EventAppeared
	require.NoError(t, err)
	require.NotNil(t, firstReadEvent)

	secondReadEvent := readConnectionClient.Recv()
	require.NoError(t, err)
	require.NotNil(t, secondReadEvent)

	// since buffer size is two, after reading two outstanding messages
	// we must acknowledge a message in order to receive third one
	protoErr := readConnectionClient.Ack(firstReadEvent)
	require.NoError(t, protoErr)

	thirdReadEvent := readConnectionClient.Recv()
	require.NotNil(t, thirdReadEvent)
}

func Test_PersistentSubscription_ToExistingStream_StartFromBeginning_AndEventsInIt(t *testing.T) {
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClient(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	// create 10 events
	events := testCreateEvents(10)

	streamID := "someStream"
	// append events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, Events);
	_, err := clientInstance.AppendToStream(context.Background(),
		streamID,
		event_streams.AppendRequestExpectedStreamRevisionNoStream{},
		events)
	require.NoError(t, err)
	// create persistent stream connection with Revision set to Start
	groupName := "Group 1"
	err = clientInstance.CreatePersistentSubscription(
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
	require.NoError(t, err)
	// read one event
	readConnectionClient, err := clientInstance.ConnectToPersistentSubscription(
		context.Background(), 10, groupName, []byte(streamID))
	require.NoError(t, err)

	readEvent := readConnectionClient.Recv().EventAppeared
	require.NoError(t, err)
	require.NotNil(t, readEvent)

	// assert Event Number == stream Start
	// assert Event.ID == first event ID (readEvent.EventID == events[0].EventID)
	require.EqualValues(t, 0, readEvent.GetOriginalEvent().EventNumber)
	require.Equal(t, events[0].EventID, readEvent.GetOriginalEvent().EventID)
}

func Test_PersistentSubscription_ToNonExistingStream_StartFromBeginning_AppendEventsAfterwards(t *testing.T) {
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClient(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	// create 10 events
	events := testCreateEvents(10)

	// create persistent stream connection with Revision set to Start
	streamID := "someStream"
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
	require.NoError(t, err)
	// append events to StreamsClient.AppendToStreamAsync(Stream, stream_revision.StreamRevisionNoStream, Events);
	_, err = clientInstance.AppendToStream(context.Background(),
		streamID,
		event_streams.AppendRequestExpectedStreamRevisionNoStream{},
		events)
	require.NoError(t, err)
	// read one event

	readConnectionClient, err := clientInstance.ConnectToPersistentSubscription(
		context.Background(), 10, groupName, []byte(streamID))
	require.NoError(t, err)

	readEvent := readConnectionClient.Recv().EventAppeared
	require.NoError(t, err)
	require.NotNil(t, readEvent)
	// assert Event Number == stream Start
	// assert Event.ID == first event ID (readEvent.EventID == events[0].EventID)
	require.EqualValues(t, 0, readEvent.GetOriginalEvent().EventNumber)
	require.Equal(t, events[0].EventID, readEvent.GetOriginalEvent().EventID)
}

func Test_PersistentSubscription_ToExistingStream_StartFromEnd_EventsInItAndAppendEventsAfterwards(t *testing.T) {
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClient(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	// create 11 events
	events := testCreateEvents(11)
	// append 10 events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, events[:10]);
	streamID := "someStream"
	// append events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, Events);
	_, err := clientInstance.AppendToStream(context.Background(),
		streamID,
		event_streams.AppendRequestExpectedStreamRevisionNoStream{},
		events[:10])
	require.NoError(t, err)
	// create persistent stream connection with Revision set to End
	groupName := "Group 1"
	err = clientInstance.CreatePersistentSubscription(
		context.Background(),
		persistent.SubscriptionStreamConfig{
			StreamOption: persistent.StreamSettings{
				StreamName: []byte(streamID),
				Revision:   persistent.Revision_End,
			},
			GroupName: groupName,
			Settings:  persistent.DefaultSubscriptionSettings,
		},
	)
	require.NoError(t, err)

	// append 1 event to StreamsClient.AppendToStreamAsync(Stream, new StreamRevision(9), event[10])
	_, err = clientInstance.AppendToStream(context.Background(),
		streamID,
		event_streams.AppendRequestExpectedStreamRevision{Revision: 9},
		events[10:])
	require.NoError(t, err)

	// read one event
	readConnectionClient, err := clientInstance.ConnectToPersistentSubscription(
		context.Background(), 10, groupName, []byte(streamID))
	require.NoError(t, err)

	readEvent := readConnectionClient.Recv().EventAppeared
	require.NoError(t, err)
	require.NotNil(t, readEvent)
	// assert readEvent.EventNumber == stream position 10
	// assert readEvent.ID == events[10].EventID
	require.EqualValues(t, 10, readEvent.GetOriginalEvent().EventNumber)
	require.Equal(t, events[10].EventID, readEvent.GetOriginalEvent().EventID)
}

func Test_PersistentSubscription_ToExistingStream_StartFromEnd_EventsInIt(t *testing.T) {
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClient(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	// create 10 events
	events := testCreateEvents(10)
	// append 10 events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, events[:10]);
	streamID := "someStream"
	// append events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, Events);
	_, err := clientInstance.AppendToStream(context.Background(),
		streamID,
		event_streams.AppendRequestExpectedStreamRevisionNoStream{},
		events[:10])
	require.NoError(t, err)
	// create persistent stream connection with position set to End
	groupName := "Group 1"
	err = clientInstance.CreatePersistentSubscription(
		context.Background(),
		persistent.SubscriptionStreamConfig{
			StreamOption: persistent.StreamSettings{
				StreamName: []byte(streamID),
				Revision:   persistent.Revision_End,
			},
			GroupName: groupName,
			Settings:  persistent.DefaultSubscriptionSettings,
		},
	)
	require.NoError(t, err)

	// reading one event after 10 seconds timeout will return no events
	ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
	readConnectionClient, err := clientInstance.ConnectToPersistentSubscription(
		ctx, 10, groupName, []byte(streamID))
	require.NoError(t, err)

	doneChannel := make(chan struct{})
	go func() {
		event := readConnectionClient.Recv().EventAppeared

		if event != nil && err == nil {
			doneChannel <- struct{}{}
		}
	}()

	noEvents := false

waitLoop:
	for {
		select {
		case <-ctx.Done():
			noEvents = true
			break waitLoop
		case <-doneChannel:
			noEvents = false
			break waitLoop
		}
	}

	require.True(t, noEvents)
	cancelFunc()
}

func Test_PersistentSubscription_ToNonExistingStream_StartFromTwo_AppendEventsAfterwards(t *testing.T) {
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClient(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	// create 3 events
	events := testCreateEvents(3)
	// create persistent stream connection with position set to Position(2)
	streamID := "someStream"
	groupName := "Group 1"
	err := clientInstance.CreatePersistentSubscription(
		context.Background(),
		persistent.SubscriptionStreamConfig{
			StreamOption: persistent.StreamSettings{
				StreamName: []byte(streamID),
				Revision:   persistent.Revision(2),
			},
			GroupName: groupName,
			Settings:  persistent.DefaultSubscriptionSettings,
		},
	)
	require.NoError(t, err)
	// append 3 event to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, events)
	_, err = clientInstance.AppendToStream(context.Background(),
		streamID,
		event_streams.AppendRequestExpectedStreamRevisionNoStream{},
		events)
	require.NoError(t, err)
	// read one event
	readConnectionClient, err := clientInstance.ConnectToPersistentSubscription(
		context.Background(), 10, groupName, []byte(streamID))
	require.NoError(t, err)
	readEvent := readConnectionClient.Recv().EventAppeared
	require.NoError(t, err)
	require.NotNil(t, readEvent)

	// assert readEvent.EventNumber == stream position 2
	// assert readEvent.ID == events[2].EventID
	require.EqualValues(t, 2, readEvent.GetOriginalEvent().EventNumber)
	require.Equal(t, events[2].EventID, readEvent.GetOriginalEvent().EventID)
}

func Test_PersistentSubscription_ToExistingStream_StartFrom10_EventsInItAppendEventsAfterwards(t *testing.T) {
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClient(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	// create 11 events
	events := testCreateEvents(11)

	// append 10 events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, events[:10]);
	streamID := "someStream"
	_, err := clientInstance.AppendToStream(context.Background(),
		streamID,
		event_streams.AppendRequestExpectedStreamRevisionNoStream{},
		events[:10])
	require.NoError(t, err)

	// create persistent stream connection with start position set to Position(10)
	groupName := "Group 1"
	err = clientInstance.CreatePersistentSubscription(
		context.Background(),
		persistent.SubscriptionStreamConfig{
			StreamOption: persistent.StreamSettings{
				StreamName: []byte(streamID),
				Revision:   persistent.Revision(10),
			},
			GroupName: groupName,
			Settings:  persistent.DefaultSubscriptionSettings,
		},
	)
	require.NoError(t, err)

	// append 1 event to StreamsClient.AppendToStreamAsync(Stream, StreamRevision(9), events[10:)
	_, err = clientInstance.AppendToStream(
		context.Background(),
		streamID,
		event_streams.AppendRequestExpectedStreamRevision{Revision: 9},
		events[10:])
	require.NoError(t, err)

	// read one event
	readConnectionClient, err := clientInstance.ConnectToPersistentSubscription(
		context.Background(), 10, groupName, []byte(streamID))
	require.NoError(t, err)
	readEvent := readConnectionClient.Recv().EventAppeared
	require.NoError(t, err)
	require.NotNil(t, readEvent)

	// assert readEvent.EventNumber == stream position 10
	// assert readEvent.ID == events[10].EventID
	require.EqualValues(t, 10, readEvent.GetOriginalEvent().EventNumber)
	require.Equal(t, events[10].EventID, readEvent.GetOriginalEvent().EventID)
}

func Test_PersistentSubscription_ToExistingStream_StartFrom4_EventsInIt(t *testing.T) {
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClient(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	// create 11 events
	events := testCreateEvents(11)

	// append 10 events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, events[:10]);
	streamID := "someStream"
	_, err := clientInstance.AppendToStream(
		context.Background(),
		streamID,
		event_streams.AppendRequestExpectedStreamRevisionNoStream{},
		events[:10])
	require.NoError(t, err)

	// create persistent stream connection with start position set to Position(4)
	groupName := "Group 1"
	err = clientInstance.CreatePersistentSubscription(
		context.Background(),
		persistent.SubscriptionStreamConfig{
			StreamOption: persistent.StreamSettings{
				StreamName: []byte(streamID),
				Revision:   persistent.Revision(4),
			},
			GroupName: groupName,
			Settings:  persistent.DefaultSubscriptionSettings,
		},
	)
	require.NoError(t, err)

	// append 1 event to StreamsClient.AppendToStreamAsync(Stream, StreamRevision(9), events)
	_, err = clientInstance.AppendToStream(
		context.Background(),
		streamID,
		event_streams.AppendRequestExpectedStreamRevision{Revision: 9},
		events[10:])
	require.NoError(t, err)

	// read one event
	readConnectionClient, err := clientInstance.ConnectToPersistentSubscription(
		context.Background(), 10, groupName, []byte(streamID))
	require.NoError(t, err)
	readEvent := readConnectionClient.Recv().EventAppeared
	require.NoError(t, err)
	require.NotNil(t, readEvent)

	// assert readEvent.EventNumber == stream position 4
	// assert readEvent.ID == events[4].EventID
	require.EqualValues(t, 4, readEvent.GetOriginalEvent().EventNumber)
	require.Equal(t, events[4].EventID, readEvent.GetOriginalEvent().EventID)
}

func Test_PersistentSubscription_ToExistingStream_StartFromHigherRevisionThenEventsInStream_EventsInItAppendEventsAfterwards(t *testing.T) {
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClient(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	// create 12 events
	events := testCreateEvents(12)

	// append 11 events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, events[:11]);
	// append 10 events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, events[:10]);
	streamID := "someStream"
	_, err := clientInstance.AppendToStream(
		context.Background(),
		streamID,
		event_streams.AppendRequestExpectedStreamRevisionNoStream{},
		events[:11])
	require.NoError(t, err)

	// create persistent stream connection with start position set to Position(11)
	groupName := "Group 1"
	err = clientInstance.CreatePersistentSubscription(
		context.Background(),
		persistent.SubscriptionStreamConfig{
			StreamOption: persistent.StreamSettings{
				StreamName: []byte(streamID),
				Revision:   persistent.Revision(11),
			},
			GroupName: groupName,
			Settings:  persistent.DefaultSubscriptionSettings,
		},
	)
	require.NoError(t, err)

	// append event to StreamsClient.AppendToStreamAsync(Stream, StreamRevision(10), events[11:])
	_, err = clientInstance.AppendToStream(
		context.Background(),
		streamID,
		event_streams.AppendRequestExpectedStreamRevision{Revision: 10},
		events[11:])
	require.NoError(t, err)

	// read one event
	readConnectionClient, err := clientInstance.ConnectToPersistentSubscription(
		context.Background(), 10, groupName, []byte(streamID))
	require.NoError(t, err)
	readEvent := readConnectionClient.Recv().EventAppeared
	require.NoError(t, err)
	require.NotNil(t, readEvent)

	// assert readEvent.EventNumber == stream position 11
	// assert readEvent.ID == events[11].EventID
	require.EqualValues(t, 11, readEvent.GetOriginalEvent().EventNumber)
	require.Equal(t, events[11].EventID, readEvent.GetOriginalEvent().EventID)
}

func Test_PersistentSubscription_ReadExistingStream_NackToReceiveNewEvents(t *testing.T) {
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClient(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	streamID := "someStream"
	firstEvent := createTestEvent()
	secondEvent := createTestEvent()
	thirdEvent := createTestEvent()
	pushEventsToStream(t, clientInstance, streamID, firstEvent, secondEvent, thirdEvent)

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

	firstReadEvent := readConnectionClient.Recv().EventAppeared
	require.NoError(t, err)
	require.NotNil(t, firstReadEvent)

	secondReadEvent := readConnectionClient.Recv()
	require.NoError(t, err)
	require.NotNil(t, secondReadEvent)

	// since buffer size is two, after reading two outstanding messages
	// we must acknowledge a message in order to receive third one
	protoErr := readConnectionClient.Nack("test reason", persistent.Nack_Park, firstReadEvent)
	require.NoError(t, protoErr)

	thirdReadEvent := readConnectionClient.Recv()
	require.NotNil(t, thirdReadEvent)
}

func Test_PersistentSubscription_ReadExistingStream_Cancelled(t *testing.T) {
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClient(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	streamID := "someStream"
	firstEvent := createTestEvent()
	pushEventsToStream(t, clientInstance, streamID, firstEvent)

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
	require.NoError(t, err)

	var bufferSize int32 = 2
	readConnectionClient, err := clientInstance.ConnectToPersistentSubscription(
		context.Background(), bufferSize, groupName, []byte(streamID))
	require.NoError(t, err)

	firstReadEvent := readConnectionClient.Recv().EventAppeared
	require.NoError(t, err)
	require.NotNil(t, firstReadEvent)

	stdErr := readConnectionClient.Close()
	require.NoError(t, stdErr)

	droppedConnectionEvent := readConnectionClient.Recv().Dropped
	require.NotNil(t, droppedConnectionEvent)
	require.Error(t, droppedConnectionEvent.Error)
}

func testCreateEvents(count uint32) []event_streams.ProposedEvent {
	result := make([]event_streams.ProposedEvent, count)
	var i uint32 = 0
	for ; i < count; i++ {
		result[i] = createTestEvent()
	}
	return result
}

func testCreateEventsWithMetadata(count uint32, metadataSize int) []event_streams.ProposedEvent {
	result := make([]event_streams.ProposedEvent, count)
	var i uint32 = 0
	for ; i < count; i++ {
		result[i] = createTestEventWithMetadataSize(metadataSize)
	}
	return result
}

func testCreateEventsWithBytesCap(bytesCap uint) []event_streams.ProposedEvent {
	byteCount := uint(0)
	result := make([]event_streams.ProposedEvent, 0)

	for {
		newEvent := createTestEvent()
		byteCount += uint(len(newEvent.Data))

		if byteCount > bytesCap {
			break
		}
		result = append(result, newEvent)
	}

	return result
}
