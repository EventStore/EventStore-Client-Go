package esdb_test

import (
	"context"
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/stretchr/testify/require"
)

func Test_PersistentSubscription_ReadExistingStream_AckToReceiveNewEvents(t *testing.T) {
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
	events := []esdb.EventData{firstEvent, secondEvent, thirdEvent}
	pushEventsToStream(t, clientInstance, streamID, events)

	groupName := "Group 1"
	err := clientInstance.CreatePersistentSubscription(
		context.Background(),
		streamID,
		groupName,
		esdb.PersistentStreamSubscriptionOptions{
			From: esdb.Start{},
		},
	)
	require.NoError(t, err)

	readConnectionClient, err := clientInstance.ConnectToPersistentSubscription(
		context.Background(), streamID, groupName, esdb.ConnectToPersistentSubscriptionOptions{
			BatchSize: 2,
		})
	require.NoError(t, err)

	firstReadEvent := readConnectionClient.Recv().EventAppeared
	require.NoError(t, err)
	require.NotNil(t, firstReadEvent)

	secondReadEvent := readConnectionClient.Recv().EventAppeared
	require.NoError(t, err)
	require.NotNil(t, secondReadEvent)

	// since buffer size is two, after reading two outstanding messages
	// we must acknowledge a message in order to receive third one
	err = readConnectionClient.Ack(firstReadEvent)
	require.NoError(t, err)

	thirdReadEvent := readConnectionClient.Recv()
	require.NoError(t, err)
	require.NotNil(t, thirdReadEvent)
}

func Test_PersistentSubscription_ToExistingStream_StartFromBeginning_AndEventsInIt(t *testing.T) {
	containerInstance, clientInstance := initializeContainerAndClient(t)
	defer func() {
		err := clientInstance.Close()
		require.NoError(t, err)
	}()
	defer containerInstance.Close()

	// create 10 events
	events := testCreateEvents(10)

	streamID := "someStream"
	// append events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, Events);
	opts := esdb.AppendToStreamOptions{
		ExpectedRevision: esdb.NoStream{},
	}

	_, err := clientInstance.AppendToStream(context.Background(), streamID, opts, events...)
	require.NoError(t, err)
	// create persistent stream connection with StreamRevision set to Start
	groupName := "Group 1"
	err = clientInstance.CreatePersistentSubscription(
		context.Background(),
		streamID,
		groupName,
		esdb.PersistentStreamSubscriptionOptions{
			From: esdb.Start{},
		},
	)
	require.NoError(t, err)
	// read one event
	readConnectionClient, err := clientInstance.ConnectToPersistentSubscription(
		context.Background(), streamID, groupName, esdb.ConnectToPersistentSubscriptionOptions{})
	require.NoError(t, err)

	readEvent := readConnectionClient.Recv().EventAppeared
	require.NoError(t, err)
	require.NotNil(t, readEvent)

	// assert Event Number == stream Start
	// assert Event.ID == first event ID (readEvent.EventID == events[0].EventID)
	require.EqualValues(t, 0, readEvent.OriginalEvent().EventNumber)
	require.Equal(t, events[0].EventID, readEvent.OriginalEvent().EventID)
}

func Test_PersistentSubscription_ToNonExistingStream_StartFromBeginning_AppendEventsAfterwards(t *testing.T) {
	containerInstance, clientInstance := initializeContainerAndClient(t)
	defer func() {
		err := clientInstance.Close()
		require.NoError(t, err)
	}()
	defer containerInstance.Close()

	// create 10 events
	events := testCreateEvents(10)

	// create persistent stream connection with StreamRevision set to Start
	streamID := "someStream"
	groupName := "Group 1"
	err := clientInstance.CreatePersistentSubscription(
		context.Background(),
		streamID,
		groupName,
		esdb.PersistentStreamSubscriptionOptions{
			From: esdb.Start{},
		},
	)
	require.NoError(t, err)
	// append events to StreamsClient.AppendToStreamAsync(Stream, stream_revision.StreamRevisionNoStream, Events);
	opts := esdb.AppendToStreamOptions{
		ExpectedRevision: esdb.NoStream{},
	}
	_, err = clientInstance.AppendToStream(context.Background(), streamID, opts, events...)
	require.NoError(t, err)
	// read one event

	readConnectionClient, err := clientInstance.ConnectToPersistentSubscription(
		context.Background(), streamID, groupName, esdb.ConnectToPersistentSubscriptionOptions{})
	require.NoError(t, err)

	readEvent := readConnectionClient.Recv().EventAppeared
	require.NoError(t, err)
	require.NotNil(t, readEvent)
	// assert Event Number == stream Start
	// assert Event.ID == first event ID (readEvent.EventID == events[0].EventID)
	require.EqualValues(t, 0, readEvent.OriginalEvent().EventNumber)
	require.Equal(t, events[0].EventID, readEvent.OriginalEvent().EventID)
}

func Test_PersistentSubscription_ToExistingStream_StartFromEnd_EventsInItAndAppendEventsAfterwards(t *testing.T) {
	containerInstance, clientInstance := initializeContainerAndClient(t)
	defer func() {
		err := clientInstance.Close()
		require.NoError(t, err)
	}()
	defer containerInstance.Close()

	// create 11 events
	events := testCreateEvents(11)
	// append 10 events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, events[:10]);
	streamID := "someStream"
	// append events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, Events);
	opts := esdb.AppendToStreamOptions{
		ExpectedRevision: esdb.NoStream{},
	}
	_, err := clientInstance.AppendToStream(context.Background(), streamID, opts, events[:10]...)
	require.NoError(t, err)
	// create persistent stream connection with StreamRevision set to End
	groupName := "Group 1"
	err = clientInstance.CreatePersistentSubscription(
		context.Background(),
		streamID,
		groupName,
		esdb.PersistentStreamSubscriptionOptions{
			From: esdb.End{},
		},
	)
	require.NoError(t, err)

	// append 1 event to StreamsClient.AppendToStreamAsync(Stream, new StreamRevision(9), event[10])
	opts.ExpectedRevision = esdb.Revision(9)
	_, err = clientInstance.AppendToStream(context.Background(), streamID, opts, events[10:]...)
	require.NoError(t, err)

	// read one event
	readConnectionClient, err := clientInstance.ConnectToPersistentSubscription(
		context.Background(), streamID, groupName, esdb.ConnectToPersistentSubscriptionOptions{})
	require.NoError(t, err)

	readEvent := readConnectionClient.Recv().EventAppeared
	require.NoError(t, err)
	require.NotNil(t, readEvent)
	// assert readEvent.EventNumber == stream From 10
	// assert readEvent.ID == events[10].EventID
	require.EqualValues(t, 10, readEvent.OriginalEvent().EventNumber)
	require.Equal(t, events[10].EventID, readEvent.OriginalEvent().EventID)
}

func Test_PersistentSubscription_ToExistingStream_StartFromEnd_EventsInIt(t *testing.T) {
	containerInstance, clientInstance := initializeContainerAndClient(t)
	defer func() {
		err := clientInstance.Close()
		require.NoError(t, err)
	}()
	defer containerInstance.Close()

	// create 10 events
	events := testCreateEvents(10)
	// append 10 events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, events[:10]);
	streamID := "someStream"
	// append events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, Events);
	opts := esdb.AppendToStreamOptions{
		ExpectedRevision: esdb.NoStream{},
	}

	_, err := clientInstance.AppendToStream(context.Background(), streamID, opts, events[:10]...)
	require.NoError(t, err)
	// create persistent stream connection with From set to End
	groupName := "Group 1"
	err = clientInstance.CreatePersistentSubscription(
		context.Background(),
		streamID,
		groupName,
		esdb.PersistentStreamSubscriptionOptions{
			From: esdb.End{},
		},
	)
	require.NoError(t, err)

	// reading one event after 10 seconds timeout will return no events
	ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
	readConnectionClient, err := clientInstance.ConnectToPersistentSubscription(
		ctx, streamID, groupName, esdb.ConnectToPersistentSubscriptionOptions{})
	require.NoError(t, err)

	doneChannel := make(chan struct{})
	go func() {
		event := readConnectionClient.Recv()

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
	containerInstance, clientInstance := initializeContainerAndClient(t)
	defer func() {
		err := clientInstance.Close()
		require.NoError(t, err)
	}()
	defer containerInstance.Close()

	// create 3 events
	events := testCreateEvents(4)
	// create persistent stream connection with From set to Position(2)
	streamID := "someStream"
	groupName := "Group 1"
	err := clientInstance.CreatePersistentSubscription(
		context.Background(),
		streamID,
		groupName,
		esdb.PersistentStreamSubscriptionOptions{
			From: esdb.Revision(2),
		},
	)
	require.NoError(t, err)
	// append 3 event to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, events)
	opts := esdb.AppendToStreamOptions{
		ExpectedRevision: esdb.NoStream{},
	}
	_, err = clientInstance.AppendToStream(context.Background(), streamID, opts, events...)
	require.NoError(t, err)
	// read one event
	readConnectionClient, err := clientInstance.ConnectToPersistentSubscription(
		context.Background(), streamID, groupName, esdb.ConnectToPersistentSubscriptionOptions{})
	require.NoError(t, err)
	readEvent := readConnectionClient.Recv().EventAppeared
	require.NoError(t, err)
	require.NotNil(t, readEvent)

	// assert readEvent.EventNumber == stream From 2
	// assert readEvent.ID == events[2].EventID
	require.EqualValues(t, 2, readEvent.OriginalEvent().EventNumber)
	require.Equal(t, events[2].EventID, readEvent.OriginalEvent().EventID)
}

func Test_PersistentSubscription_ToExistingStream_StartFrom10_EventsInItAppendEventsAfterwards(t *testing.T) {
	containerInstance, clientInstance := initializeContainerAndClient(t)
	defer func() {
		err := clientInstance.Close()
		require.NoError(t, err)
	}()
	defer containerInstance.Close()

	// create 11 events
	events := testCreateEvents(11)

	// append 10 events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, events[:10]);
	opts := esdb.AppendToStreamOptions{
		ExpectedRevision: esdb.NoStream{},
	}
	streamID := "someStream"
	_, err := clientInstance.AppendToStream(context.Background(), streamID, opts, events[:10]...)
	require.NoError(t, err)

	// create persistent stream connection with start From set to Position(10)
	groupName := "Group 1"
	err = clientInstance.CreatePersistentSubscription(
		context.Background(),
		streamID,
		groupName,
		esdb.PersistentStreamSubscriptionOptions{
			From: esdb.Revision(10),
		},
	)
	require.NoError(t, err)

	// append 1 event to StreamsClient.AppendToStreamAsync(Stream, StreamRevision(9), events[10:)
	opts = esdb.AppendToStreamOptions{
		ExpectedRevision: esdb.Revision(9),
	}
	_, err = clientInstance.AppendToStream(context.Background(), streamID, opts, events[10:]...)
	require.NoError(t, err)

	// read one event
	readConnectionClient, err := clientInstance.ConnectToPersistentSubscription(
		context.Background(), streamID, groupName, esdb.ConnectToPersistentSubscriptionOptions{})
	require.NoError(t, err)
	readEvent := readConnectionClient.Recv().EventAppeared
	require.NoError(t, err)
	require.NotNil(t, readEvent)

	// assert readEvent.EventNumber == stream From 10
	// assert readEvent.ID == events[10].EventID
	require.EqualValues(t, 10, readEvent.OriginalEvent().EventNumber)
	require.Equal(t, events[10].EventID, readEvent.OriginalEvent().EventID)
}

func Test_PersistentSubscription_ToExistingStream_StartFrom4_EventsInIt(t *testing.T) {
	containerInstance, clientInstance := initializeContainerAndClient(t)
	defer func() {
		err := clientInstance.Close()
		require.NoError(t, err)
	}()
	defer containerInstance.Close()

	// create 11 events
	events := testCreateEvents(11)

	// append 10 events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, events[:10]);
	opts := esdb.AppendToStreamOptions{
		ExpectedRevision: esdb.NoStream{},
	}
	streamID := "someStream"
	_, err := clientInstance.AppendToStream(context.Background(), streamID, opts, events[:10]...)
	require.NoError(t, err)

	// create persistent stream connection with start From set to Position(4)
	groupName := "Group 1"

	err = clientInstance.CreatePersistentSubscription(
		context.Background(),
		streamID,
		groupName,
		esdb.PersistentStreamSubscriptionOptions{
			From: esdb.Revision(4),
		},
	)
	require.NoError(t, err)

	// append 1 event to StreamsClient.AppendToStreamAsync(Stream, StreamRevision(9), events)
	opts = esdb.AppendToStreamOptions{
		ExpectedRevision: esdb.Revision(9),
	}
	_, err = clientInstance.AppendToStream(context.Background(), streamID, opts, events[10:]...)
	require.NoError(t, err)

	// read one event
	readConnectionClient, err := clientInstance.ConnectToPersistentSubscription(
		context.Background(), streamID, groupName, esdb.ConnectToPersistentSubscriptionOptions{})
	require.NoError(t, err)
	readEvent := readConnectionClient.Recv().EventAppeared
	require.NoError(t, err)
	require.NotNil(t, readEvent)

	// assert readEvent.EventNumber == stream From 4
	// assert readEvent.ID == events[4].EventID
	require.EqualValues(t, 4, readEvent.OriginalEvent().EventNumber)
	require.Equal(t, events[4].EventID, readEvent.OriginalEvent().EventID)
}

func Test_PersistentSubscription_ToExistingStream_StartFromHigherRevisionThenEventsInStream_EventsInItAppendEventsAfterwards(t *testing.T) {
	containerInstance, clientInstance := initializeContainerAndClient(t)
	defer func() {
		err := clientInstance.Close()
		require.NoError(t, err)
	}()
	defer containerInstance.Close()

	// create 12 events
	events := testCreateEvents(12)

	// append 11 events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, events[:11]);
	// append 10 events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, events[:10]);
	streamID := "someStream"
	opts := esdb.AppendToStreamOptions{
		ExpectedRevision: esdb.NoStream{},
	}
	_, err := clientInstance.AppendToStream(context.Background(), streamID, opts, events[:11]...)
	require.NoError(t, err)

	// create persistent stream connection with start From set to Position(11)
	groupName := "Group 1"
	err = clientInstance.CreatePersistentSubscription(
		context.Background(),
		streamID,
		groupName,
		esdb.PersistentStreamSubscriptionOptions{
			From: esdb.Revision(11),
		},
	)
	require.NoError(t, err)

	// append event to StreamsClient.AppendToStreamAsync(Stream, StreamRevision(10), events[11:])
	opts = esdb.AppendToStreamOptions{
		ExpectedRevision: esdb.Revision(10),
	}

	_, err = clientInstance.AppendToStream(context.Background(), streamID, opts, events[11])
	require.NoError(t, err)

	// read one event
	readConnectionClient, err := clientInstance.ConnectToPersistentSubscription(
		context.Background(), streamID, groupName, esdb.ConnectToPersistentSubscriptionOptions{})
	require.NoError(t, err)
	readEvent := readConnectionClient.Recv().EventAppeared
	require.NoError(t, err)
	require.NotNil(t, readEvent)

	// assert readEvent.EventNumber == stream From 11
	// assert readEvent.ID == events[11].EventID
	require.EqualValues(t, 11, readEvent.OriginalEvent().EventNumber)
	require.Equal(t, events[11].EventID, readEvent.OriginalEvent().EventID)
}

func Test_PersistentSubscription_ReadExistingStream_NackToReceiveNewEvents(t *testing.T) {
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
	events := []esdb.EventData{firstEvent, secondEvent, thirdEvent}
	pushEventsToStream(t, clientInstance, streamID, events)

	groupName := "Group 1"
	err := clientInstance.CreatePersistentSubscription(
		context.Background(),
		streamID,
		groupName,
		esdb.PersistentStreamSubscriptionOptions{
			From: esdb.Start{},
		},
	)

	readConnectionClient, err := clientInstance.ConnectToPersistentSubscription(
		context.Background(), streamID, groupName, esdb.ConnectToPersistentSubscriptionOptions{
			BatchSize: 2,
		})
	require.NoError(t, err)

	firstReadEvent := readConnectionClient.Recv().EventAppeared
	require.NoError(t, err)
	require.NotNil(t, firstReadEvent)

	secondReadEvent := readConnectionClient.Recv().EventAppeared
	require.NoError(t, err)
	require.NotNil(t, secondReadEvent)

	// since buffer size is two, after reading two outstanding messages
	// we must acknowledge a message in order to receive third one
	err = readConnectionClient.Nack("test reason", esdb.Nack_Park, firstReadEvent)
	require.NoError(t, err)

	thirdReadEvent := readConnectionClient.Recv()
	require.NoError(t, err)
	require.NotNil(t, thirdReadEvent)
}

func testCreateEvents(count uint32) []esdb.EventData {
	result := make([]esdb.EventData, count)
	var i uint32 = 0
	for ; i < count; i++ {
		result[i] = createTestEvent()
	}
	return result
}
