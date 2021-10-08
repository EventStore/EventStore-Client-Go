package persistent_integration_test

import (
	"context"
	"testing"
	"time"

	"github.com/pivonroll/EventStore-Client-Go/errors"
	"github.com/pivonroll/EventStore-Client-Go/event_streams"
	"github.com/pivonroll/EventStore-Client-Go/persistent"
	"github.com/stretchr/testify/require"
)

func Test_PersistentSubscription_ReadExistingStream(t *testing.T) {
	client, eventStreamsClient, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	t.Run("AckToReceiveNewEvents", func(t *testing.T) {
		streamID := "AckToReceiveNewEvents"
		firstEvent := testCreateEvent()
		secondEvent := testCreateEvent()
		thirdEvent := testCreateEvent()
		pushEventsToStream(t, eventStreamsClient, streamID, firstEvent, secondEvent, thirdEvent)

		groupName := "GroupAckToReceiveNewEvents"
		request := persistent.CreateOrUpdateStreamRequest{
			StreamName: streamID,
			GroupName:  groupName,
			Revision:   persistent.StreamRevisionStart{},
			Settings:   persistent.DefaultRequestSettings,
		}
		err := client.CreateStreamSubscription(
			context.Background(),
			request,
		)
		require.NoError(t, err)

		var bufferSize int32 = 2
		readConnectionClient, err := client.
			SubscribeToStreamSync(context.Background(), bufferSize, groupName, streamID)
		require.NoError(t, err)

		firstReadEvent, err := readConnectionClient.ReadOne()
		require.NoError(t, err)
		require.NotNil(t, firstReadEvent.Event)

		secondReadEvent, err := readConnectionClient.ReadOne()
		require.NoError(t, err)
		require.NotNil(t, secondReadEvent.Event)

		// since buffer size is two, after reading two outstanding messages
		// we must acknowledge a message in order to receive third one
		err = readConnectionClient.Ack(firstReadEvent)
		require.NoError(t, err)

		thirdReadEvent, err := readConnectionClient.ReadOne()
		require.NoError(t, err)
		require.NotNil(t, thirdReadEvent.Event)
	})

	t.Run("NackToReceiveNewEvents", func(t *testing.T) {
		streamID := "NackToReceiveNewEvents"
		firstEvent := testCreateEvent()
		secondEvent := testCreateEvent()
		thirdEvent := testCreateEvent()
		pushEventsToStream(t, eventStreamsClient, streamID, firstEvent, secondEvent, thirdEvent)

		groupName := "GroupNackToReceiveNewEvents"
		request := persistent.CreateOrUpdateStreamRequest{
			StreamName: streamID,
			GroupName:  groupName,
			Revision:   persistent.StreamRevisionStart{},
			Settings:   persistent.DefaultRequestSettings,
		}
		err := client.CreateStreamSubscription(
			context.Background(),
			request,
		)

		var bufferSize int32 = 2
		readConnectionClient, err := client.
			SubscribeToStreamSync(context.Background(), bufferSize, groupName, streamID)
		require.NoError(t, err)

		firstReadEvent, err := readConnectionClient.ReadOne()
		require.NoError(t, err)
		require.NotNil(t, firstReadEvent.Event)

		secondReadEvent, err := readConnectionClient.ReadOne()
		require.NoError(t, err)
		require.NotNil(t, secondReadEvent.Event)

		// since buffer size is two, after reading two outstanding messages
		// we must acknowledge a message in order to receive third one
		protoErr := readConnectionClient.Nack("test reason", persistent.Nack_Park, firstReadEvent)
		require.NoError(t, protoErr)

		thirdReadEvent, err := readConnectionClient.ReadOne()
		require.NoError(t, err)
		require.NotNil(t, thirdReadEvent.Event)
	})

	t.Run("NackToReceiveNewEvents Cancelled", func(t *testing.T) {
		t.Skip()
		streamID := "NackToReceiveNewEvents_Cancelled"
		firstEvent := testCreateEvent()
		pushEventsToStream(t, eventStreamsClient, streamID, firstEvent)

		groupName := "GroupNackToReceiveNewEvents_Cancelled"
		request := persistent.CreateOrUpdateStreamRequest{
			StreamName: streamID,
			GroupName:  groupName,
			Revision:   persistent.StreamRevisionStart{},
			Settings:   persistent.DefaultRequestSettings,
		}
		err := client.CreateStreamSubscription(
			context.Background(),
			request,
		)
		require.NoError(t, err)

		var bufferSize int32 = 2
		readConnectionClient, err := client.
			SubscribeToStreamSync(context.Background(), bufferSize, groupName, streamID)
		require.NoError(t, err)

		firstReadEvent, err := readConnectionClient.ReadOne()
		require.NoError(t, err)
		require.NotNil(t, firstReadEvent.Event)

		readConnectionClient.Close()

		droppedConnectionEvent, err := readConnectionClient.ReadOne()
		require.Equal(t, errors.CanceledErr, err.Code())
		require.Empty(t, droppedConnectionEvent)
	})
}

func Test_PersistentSubscription_OldConnectionsAreDroppedAfterUpdate(t *testing.T) {
	t.Skip("Fails on github actions while waiting for grpc headers when updating subscription")
	client, eventStreamsClient, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	streamID := "PersistentSubscriptionsOldConnectionsAreDropped"
	firstEvent := testCreateEvent()
	secondEvent := testCreateEvent()
	thirdEvent := testCreateEvent()
	pushEventsToStream(t, eventStreamsClient, streamID, firstEvent, secondEvent, thirdEvent)

	groupName := "GroupOldConnectionsAreDropped"
	request := persistent.CreateOrUpdateStreamRequest{
		StreamName: streamID,
		GroupName:  groupName,
		Revision:   persistent.StreamRevisionStart{},
		Settings:   persistent.DefaultRequestSettings,
	}
	err := client.CreateStreamSubscription(
		context.Background(),
		request,
	)
	require.NoError(t, err)

	var bufferSize int32 = 2
	readConnectionClient, err := client.
		SubscribeToStreamSync(context.Background(), bufferSize, groupName, streamID)
	require.NoError(t, err)

	firstReadEvent, err := readConnectionClient.ReadOne()
	require.NoError(t, err)
	require.NotNil(t, firstReadEvent.Event)

	secondReadEvent, err := readConnectionClient.ReadOne()
	require.NoError(t, err)
	require.NotNil(t, secondReadEvent)

	// ack second message
	err = readConnectionClient.Ack(secondReadEvent)
	require.NoError(t, err)

	oldReadConnection := readConnectionClient

	err = client.UpdateStreamSubscription(context.Background(),
		request)
	require.NoError(t, err)

	// subscribe to stream again (continue to receive events)
	_, err = client.
		SubscribeToStreamSync(context.Background(), bufferSize, groupName, streamID)
	require.NoError(t, err)

	attempts := 10
	counter := 0
	for ; counter < attempts; counter++ {
		_, err = oldReadConnection.ReadOne()

		if err != nil {
			require.Equal(t, errors.PersistentSubscriptionDroppedErr, err.Code())
			break
		}

		time.Sleep(1 * time.Second)
	}

	require.NotEqual(t, 10, counter)
}

func Test_PersistentSubscription_AckToReceiveNewEventsStartFromSamePositionWithReconnect(t *testing.T) {
	t.Skip("Skipping cause updating persistent subscription can block on awaiting grpc headers")
	client, eventStreamsClient, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	streamID := "AckToReceiveNewEventsStartFromSamePositionWithReconnect"
	firstEvent := testCreateEvent()
	secondEvent := testCreateEvent()
	thirdEvent := testCreateEvent()
	pushEventsToStream(t, eventStreamsClient, streamID, firstEvent, secondEvent, thirdEvent)

	groupName := "GroupAckToReceiveNewEventsWithReconnectFromStart"
	request := persistent.CreateOrUpdateStreamRequest{
		StreamName: streamID,
		GroupName:  groupName,
		Revision:   persistent.StreamRevisionStart{},
		Settings:   persistent.DefaultRequestSettings,
	}
	err := client.CreateStreamSubscription(
		context.Background(),
		request,
	)
	require.NoError(t, err)

	var bufferSize int32 = 2
	readConnectionClient, err := client.
		SubscribeToStreamSync(context.Background(), bufferSize, groupName, streamID)
	require.NoError(t, err)

	firstReadEvent, err := readConnectionClient.ReadOne()
	require.NoError(t, err)
	require.NotNil(t, firstReadEvent.Event)

	secondReadEvent, err := readConnectionClient.ReadOne()
	require.NoError(t, err)
	require.NotNil(t, secondReadEvent)

	// ack second message
	err = readConnectionClient.Ack(secondReadEvent)
	require.NoError(t, err)

	request.Revision = persistent.StreamRevision{Revision: 1}
	err = client.UpdateStreamSubscription(context.Background(),
		request)
	require.NoError(t, err)

	// subscribe to stream again start from beginning
	readConnectionClient, err = client.
		SubscribeToStreamSync(context.Background(), bufferSize, groupName, streamID)
	require.NoError(t, err)

	firstReadEvent, err = readConnectionClient.ReadOne()
	require.NoError(t, err)
	require.NotNil(t, firstReadEvent.Event)
	require.Equal(t, secondEvent.EventId, firstReadEvent.Event.EventID)

	secondReadEvent, err = readConnectionClient.ReadOne()
	require.NoError(t, err)
	require.NotNil(t, secondReadEvent)
	require.Equal(t, thirdEvent.EventId, secondReadEvent.Event.EventID)
}

func Test_PersistentSubscription_AckToReceiveNewEventsWithReconnect_Bug_ReceivesFirstEvent(t *testing.T) {
	client, eventStreamsClient, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	streamID := "AckToReceiveNewEventsWithReconnect"
	firstEvent := testCreateEvent()
	secondEvent := testCreateEvent()
	thirdEvent := testCreateEvent()

	pushEventsToStream(t, eventStreamsClient, streamID, firstEvent, secondEvent, thirdEvent)

	settings := persistent.DefaultRequestSettings
	settings.MinCheckpointCount = 2

	groupName := "GroupAckToReceiveNewEventsWithReconnect"
	request := persistent.CreateOrUpdateStreamRequest{
		StreamName: streamID,
		GroupName:  groupName,
		Revision:   persistent.StreamRevisionStart{},
		Settings:   settings,
	}
	err := client.CreateStreamSubscription(
		context.Background(),
		request,
	)
	require.NoError(t, err)

	var bufferSize int32 = 2
	readConnectionClient, err := client.
		SubscribeToStreamSync(context.Background(), bufferSize, groupName, streamID)
	require.NoError(t, err)

	firstReadEvent, err := readConnectionClient.ReadOne()
	require.NoError(t, err)
	require.NotNil(t, firstReadEvent.Event)

	err = readConnectionClient.Ack(firstReadEvent)
	require.NoError(t, err)

	secondReadEvent, err := readConnectionClient.ReadOne()
	require.NoError(t, err)
	require.NotNil(t, secondReadEvent.Event)

	// ack second message
	err = readConnectionClient.Ack(secondReadEvent)
	require.NoError(t, err)

	time.Sleep(3 * time.Second)
	readConnectionClient.Close()

	// wait for some time for connection to actually close
	time.Sleep(5 * time.Second)

	readConnectionClient, err = client.
		SubscribeToStreamSync(context.Background(), bufferSize, groupName, streamID)
	require.NoError(t, err)

	thirdReadEvent, err := readConnectionClient.ReadOne()
	require.NoError(t, err)
	err = readConnectionClient.Ack(thirdReadEvent)
	require.NoError(t, err)

	require.NotNil(t, thirdReadEvent.Event)
	require.Equal(t, thirdEvent.EventId, thirdReadEvent.Event.EventID)
}

func Test_PersistentSubscription_ToNonExistingStream(t *testing.T) {
	client, eventStreamsClient, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	t.Run("StartFromBeginning_AppendEventsAfterwards", func(t *testing.T) {
		// create 10 events
		events := testCreateEvents(10)

		// create persistent stream connection with Revision set to Start
		streamID := "StartFromBeginning_AppendEventsAfterwards"
		groupName := "Group StartFromBeginning_AppendEventsAfterwards"
		request := persistent.CreateOrUpdateStreamRequest{
			StreamName: streamID,
			GroupName:  groupName,
			Revision:   persistent.StreamRevisionStart{},
			Settings:   persistent.DefaultRequestSettings,
		}
		err := client.CreateStreamSubscription(
			context.Background(),
			request,
		)
		require.NoError(t, err)
		pushEventsToStream(t, eventStreamsClient, streamID, events...)

		readConnectionClient, err := client.
			SubscribeToStreamSync(context.Background(), 10, groupName, streamID)
		require.NoError(t, err)

		readEvent, err := readConnectionClient.ReadOne()
		require.NoError(t, err)
		require.NotNil(t, readEvent.Event)
		// assert Event Number == stream Start
		// assert Event.ID == first event ID (readEvent.EventId == events[0].EventId)
		require.EqualValues(t, 0, readEvent.GetOriginalEvent().EventNumber)
		require.Equal(t, events[0].EventId, readEvent.GetOriginalEvent().EventID)
	})

	t.Run("StartFromTwo_AppendEventsAfterwards", func(t *testing.T) {
		// create 3 events
		events := testCreateEvents(3)
		// create persistent stream connection with position set to Position(2)
		streamID := "StartFromTwo_AppendEventsAfterwards"
		groupName := "Group StartFromTwo_AppendEventsAfterwards"
		request := persistent.CreateOrUpdateStreamRequest{
			StreamName: streamID,
			GroupName:  groupName,
			Revision:   persistent.StreamRevision{Revision: 2},
			Settings:   persistent.DefaultRequestSettings,
		}
		err := client.CreateStreamSubscription(
			context.Background(),
			request,
		)
		require.NoError(t, err)
		// append 3 event to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, events)
		pushEventsToStream(t, eventStreamsClient, streamID, events...)

		// read one event
		readConnectionClient, err := client.
			SubscribeToStreamSync(context.Background(), 10, groupName, streamID)
		require.NoError(t, err)
		readEvent, err := readConnectionClient.ReadOne()
		require.NoError(t, err)
		require.NotNil(t, readEvent.Event)

		// assert readEvent.EventNumber == stream position 2
		// assert readEvent.ID == events[2].EventId
		require.EqualValues(t, 2, readEvent.GetOriginalEvent().EventNumber)
		require.Equal(t, events[2].EventId, readEvent.GetOriginalEvent().EventID)
	})
}

func Test_PersistentSubscription_ToExistingStream(t *testing.T) {
	client, eventStreamsClient, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	t.Run("StartFromBeginning_AndEventsInIt", func(t *testing.T) {
		// create 10 events
		events := testCreateEvents(10)

		streamID := "StartFromBeginning_AndEventsInIt"
		// append events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, Events);
		pushEventsToStream(t, eventStreamsClient, streamID, events...)

		// create persistent stream connection with Revision set to Start
		groupName := "Group StartFromBeginning_AndEventsInIt"
		request := persistent.CreateOrUpdateStreamRequest{
			StreamName: streamID,
			GroupName:  groupName,
			Revision:   persistent.StreamRevisionStart{},
			Settings:   persistent.DefaultRequestSettings,
		}
		err := client.CreateStreamSubscription(
			context.Background(),
			request,
		)
		require.NoError(t, err)
		// read one event
		readConnectionClient, err := client.
			SubscribeToStreamSync(context.Background(), 10, groupName, streamID)
		require.NoError(t, err)

		readEvent, err := readConnectionClient.ReadOne()
		require.NoError(t, err)
		require.NotNil(t, readEvent.Event)

		// assert Event Number == stream Start
		// assert Event.ID == first event ID (readEvent.EventId == events[0].EventId)
		require.EqualValues(t, 0, readEvent.GetOriginalEvent().EventNumber)
		require.Equal(t, events[0].EventId, readEvent.GetOriginalEvent().EventID)
	})

	t.Run("StartFromEnd_EventsInItAndAppendEventsAfterwards", func(t *testing.T) {
		// create 11 events
		events := testCreateEvents(11)
		// append 10 events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, events[:10]);
		streamID := "StartFromEnd_EventsInItAndAppendEventsAfterwards"
		// append events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, Events);
		pushEventsToStream(t, eventStreamsClient, streamID, events[:10]...)

		// create persistent stream connection with Revision set to End
		groupName := "Group StartFromEnd_EventsInItAndAppendEventsAfterwards"
		request := persistent.CreateOrUpdateStreamRequest{
			StreamName: streamID,
			GroupName:  groupName,
			Revision:   persistent.StreamRevisionEnd{},
			Settings:   persistent.DefaultRequestSettings,
		}
		err := client.CreateStreamSubscription(
			context.Background(),
			request,
		)
		require.NoError(t, err)

		// append 1 event to StreamsClient.AppendToStreamAsync(Stream, new WriteStreamRevision(9), event[10])
		_, err = eventStreamsClient.AppendToStream(context.Background(),
			streamID,
			event_streams.WriteStreamRevision{Revision: 9},
			events[10:])
		require.NoError(t, err)

		// read one event
		readConnectionClient, err := client.
			SubscribeToStreamSync(context.Background(), 10, groupName, streamID)
		require.NoError(t, err)

		readEvent, err := readConnectionClient.ReadOne()
		require.NoError(t, err)
		require.NotNil(t, readEvent)
		// assert readEvent.EventNumber == stream position 10
		// assert readEvent.ID == events[10].EventId
		require.EqualValues(t, 10, readEvent.GetOriginalEvent().EventNumber)
		require.Equal(t, events[10].EventId, readEvent.GetOriginalEvent().EventID)
	})

	t.Run("StartFromEnd_EventsInIt", func(t *testing.T) {
		// create 10 events
		events := testCreateEvents(10)
		// append 10 events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, events[:10]);
		streamID := "StartFromEnd_EventsInIt"
		// append events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, Events);
		pushEventsToStream(t, eventStreamsClient, streamID, events[:10]...)
		// create persistent stream connection with position set to End
		groupName := "Group StartFromEnd_EventsInIt"
		request := persistent.CreateOrUpdateStreamRequest{
			StreamName: streamID,
			GroupName:  groupName,
			Revision:   persistent.StreamRevisionEnd{},
			Settings:   persistent.DefaultRequestSettings,
		}
		err := client.CreateStreamSubscription(
			context.Background(),
			request,
		)
		require.NoError(t, err)

		// reading one event after 10 seconds timeout will return no events
		ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
		readConnectionClient, err := client.
			SubscribeToStreamSync(ctx, 10, groupName, streamID)
		require.NoError(t, err)

		doneChannel := make(chan struct{})
		go func() {
			event, _ := readConnectionClient.ReadOne()

			if event.Event != nil && err == nil {
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
	})

	t.Run("StartFrom10_EventsInItAppendEventsAfterwards", func(t *testing.T) {
		// create 11 events
		events := testCreateEvents(11)

		// append 10 events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, events[:10]);
		streamID := "StartFrom10_EventsInItAppendEventsAfterwards"
		pushEventsToStream(t, eventStreamsClient, streamID, events[:10]...)

		// create persistent stream connection with start position set to Position(10)
		groupName := "Group StartFrom10_EventsInItAppendEventsAfterwards"
		request := persistent.CreateOrUpdateStreamRequest{
			StreamName: streamID,
			GroupName:  groupName,
			Revision:   persistent.StreamRevision{Revision: 10},
			Settings:   persistent.DefaultRequestSettings,
		}
		err := client.CreateStreamSubscription(
			context.Background(),
			request,
		)
		require.NoError(t, err)

		// append 1 event to StreamsClient.AppendToStreamAsync(Stream, WriteStreamRevision(9), events[10:)
		_, err = eventStreamsClient.AppendToStream(
			context.Background(),
			streamID,
			event_streams.WriteStreamRevision{Revision: 9},
			events[10:])
		require.NoError(t, err)

		// read one event
		readConnectionClient, err := client.
			SubscribeToStreamSync(context.Background(), 10, groupName, streamID)
		require.NoError(t, err)
		readEvent, err := readConnectionClient.ReadOne()
		require.NoError(t, err)
		require.NotNil(t, readEvent.Event)

		// assert readEvent.EventNumber == stream position 10
		// assert readEvent.ID == events[10].EventId
		require.EqualValues(t, 10, readEvent.GetOriginalEvent().EventNumber)
		require.Equal(t, events[10].EventId, readEvent.GetOriginalEvent().EventID)
	})

	t.Run("StartFrom4_EventsInIt", func(t *testing.T) {
		// create 11 events
		events := testCreateEvents(11)

		// append 10 events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, events[:10]);
		streamID := "StartFrom4_EventsInIt"
		pushEventsToStream(t, eventStreamsClient, streamID, events[:10]...)

		// create persistent stream connection with start position set to Position(4)
		groupName := "Group StartFrom4_EventsInIt"
		request := persistent.CreateOrUpdateStreamRequest{
			StreamName: streamID,
			GroupName:  groupName,
			Revision:   persistent.StreamRevision{Revision: 4},
			Settings:   persistent.DefaultRequestSettings,
		}
		err := client.CreateStreamSubscription(
			context.Background(),
			request,
		)
		require.NoError(t, err)

		// append 1 event to StreamsClient.AppendToStreamAsync(Stream, WriteStreamRevision(9), events)
		_, err = eventStreamsClient.AppendToStream(
			context.Background(),
			streamID,
			event_streams.WriteStreamRevision{Revision: 9},
			events[10:])
		require.NoError(t, err)

		// read one event
		readConnectionClient, err := client.
			SubscribeToStreamSync(context.Background(), 10, groupName, streamID)
		require.NoError(t, err)
		readEvent, err := readConnectionClient.ReadOne()
		require.NoError(t, err)
		require.NotNil(t, readEvent.Event)

		// assert readEvent.EventNumber == stream position 4
		// assert readEvent.ID == events[4].EventId
		require.EqualValues(t, 4, readEvent.GetOriginalEvent().EventNumber)
		require.Equal(t, events[4].EventId, readEvent.GetOriginalEvent().EventID)
	})

	t.Run("StartFromHigherRevisionThenEventsInStream_EventsInItAppendEventsAfterwards", func(t *testing.T) {
		// create 12 events
		events := testCreateEvents(12)

		// append 11 events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, events[:11]);
		// append 10 events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, events[:10]);
		streamID := "StartFromHigherRevisionThenEventsInStream_EventsInItAppendEventsAfterwards"
		pushEventsToStream(t, eventStreamsClient, streamID, events[:11]...)

		// create persistent stream connection with start position set to Position(11)
		groupName := "Group StartFromHigherRevisionThenEventsInStream_EventsInItAppendEventsAfterwards"
		request := persistent.CreateOrUpdateStreamRequest{
			StreamName: streamID,
			GroupName:  groupName,
			Revision:   persistent.StreamRevision{Revision: 11},
			Settings:   persistent.DefaultRequestSettings,
		}
		err := client.CreateStreamSubscription(
			context.Background(),
			request,
		)
		require.NoError(t, err)

		// append event to StreamsClient.AppendToStreamAsync(Stream, WriteStreamRevision(10), events[11:])
		_, err = eventStreamsClient.AppendToStream(
			context.Background(),
			streamID,
			event_streams.WriteStreamRevision{Revision: 10},
			events[11:])
		require.NoError(t, err)

		// read one event
		readConnectionClient, err := client.
			SubscribeToStreamSync(context.Background(), 10, groupName, streamID)
		require.NoError(t, err)
		readEvent, err := readConnectionClient.ReadOne()
		require.NoError(t, err)
		require.NotNil(t, readEvent.Event)

		// assert readEvent.EventNumber == stream position 11
		// assert readEvent.ID == events[11].EventId
		require.EqualValues(t, 11, readEvent.GetOriginalEvent().EventNumber)
		require.Equal(t, events[11].EventId, readEvent.GetOriginalEvent().EventID)
	})
}
