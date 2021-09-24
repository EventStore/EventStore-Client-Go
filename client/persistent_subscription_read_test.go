package client_test

import (
	"context"
	"testing"
	"time"

	"github.com/pivonroll/EventStore-Client-Go/event_streams"
	"github.com/pivonroll/EventStore-Client-Go/persistent"
	"github.com/stretchr/testify/require"
)

func Test_PersistentSubscription_ReadExistingStream(t *testing.T) {
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClient(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	t.Run("AckToReceiveNewEvents", func(t *testing.T) {
		streamID := "AckToReceiveNewEvents"
		firstEvent := testCreateEvent()
		secondEvent := testCreateEvent()
		thirdEvent := testCreateEvent()
		pushEventsToStream(t, clientInstance, streamID, firstEvent, secondEvent, thirdEvent)

		groupName := "Group AckToReceiveNewEvents"
		request := persistent.CreateOrUpdateStreamRequest{
			StreamName: streamID,
			GroupName:  groupName,
			Revision:   persistent.StreamRevisionStart{},
			Settings:   persistent.DefaultRequestSettings,
		}
		err := clientInstance.PersistentSubscriptions().CreateStreamSubscription(
			context.Background(),
			request,
		)
		require.NoError(t, err)

		var bufferSize int32 = 2
		readConnectionClient, err := clientInstance.PersistentSubscriptions().
			SubscribeToStreamSync(context.Background(), bufferSize, groupName, streamID)
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
	})

	t.Run("NackToReceiveNewEvents", func(t *testing.T) {
		streamID := "NackToReceiveNewEvents"
		firstEvent := testCreateEvent()
		secondEvent := testCreateEvent()
		thirdEvent := testCreateEvent()
		pushEventsToStream(t, clientInstance, streamID, firstEvent, secondEvent, thirdEvent)

		groupName := "Group NackToReceiveNewEvents"
		request := persistent.CreateOrUpdateStreamRequest{
			StreamName: streamID,
			GroupName:  groupName,
			Revision:   persistent.StreamRevisionStart{},
			Settings:   persistent.DefaultRequestSettings,
		}
		err := clientInstance.PersistentSubscriptions().CreateStreamSubscription(
			context.Background(),
			request,
		)

		var bufferSize int32 = 2
		readConnectionClient, err := clientInstance.PersistentSubscriptions().
			SubscribeToStreamSync(context.Background(), bufferSize, groupName, streamID)
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
	})

	t.Run("NackToReceiveNewEvents Cancelled", func(t *testing.T) {
		streamID := "NackToReceiveNewEvents_Cancelled"
		firstEvent := testCreateEvent()
		pushEventsToStream(t, clientInstance, streamID, firstEvent)

		groupName := "Group NackToReceiveNewEvents_Cancelled"
		request := persistent.CreateOrUpdateStreamRequest{
			StreamName: streamID,
			GroupName:  groupName,
			Revision:   persistent.StreamRevisionStart{},
			Settings:   persistent.DefaultRequestSettings,
		}
		err := clientInstance.PersistentSubscriptions().CreateStreamSubscription(
			context.Background(),
			request,
		)
		require.NoError(t, err)

		var bufferSize int32 = 2
		readConnectionClient, err := clientInstance.PersistentSubscriptions().
			SubscribeToStreamSync(context.Background(), bufferSize, groupName, streamID)
		require.NoError(t, err)

		firstReadEvent := readConnectionClient.Recv().EventAppeared
		require.NoError(t, err)
		require.NotNil(t, firstReadEvent)

		stdErr := readConnectionClient.Close()
		require.NoError(t, stdErr)

		droppedConnectionEvent := readConnectionClient.Recv().Dropped
		require.NotNil(t, droppedConnectionEvent)
		require.Error(t, droppedConnectionEvent.Error)
	})
}

func Test_PersistentSubscription_ToNonExistingStream(t *testing.T) {
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClient(t)
	defer closeClientInstance()
	defer containerInstance.Close()

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
		err := clientInstance.PersistentSubscriptions().CreateStreamSubscription(
			context.Background(),
			request,
		)
		require.NoError(t, err)
		// append events to StreamsClient.AppendToStreamAsync(Stream, stream_revision.StreamRevisionNoStream, Events);
		_, err = clientInstance.EventStreams().AppendToStream(context.Background(),
			streamID,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			events)
		require.NoError(t, err)
		// read one event

		readConnectionClient, err := clientInstance.PersistentSubscriptions().
			SubscribeToStreamSync(context.Background(), 10, groupName, streamID)
		require.NoError(t, err)

		readEvent := readConnectionClient.Recv().EventAppeared
		require.NoError(t, err)
		require.NotNil(t, readEvent)
		// assert Event Number == stream Start
		// assert Event.ID == first event ID (readEvent.EventID == events[0].EventID)
		require.EqualValues(t, 0, readEvent.GetOriginalEvent().EventNumber)
		require.Equal(t, events[0].EventID, readEvent.GetOriginalEvent().EventID)
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
		err := clientInstance.PersistentSubscriptions().CreateStreamSubscription(
			context.Background(),
			request,
		)
		require.NoError(t, err)
		// append 3 event to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, events)
		_, err = clientInstance.EventStreams().AppendToStream(context.Background(),
			streamID,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			events)
		require.NoError(t, err)
		// read one event
		readConnectionClient, err := clientInstance.PersistentSubscriptions().
			SubscribeToStreamSync(context.Background(), 10, groupName, streamID)
		require.NoError(t, err)
		readEvent := readConnectionClient.Recv().EventAppeared
		require.NoError(t, err)
		require.NotNil(t, readEvent)

		// assert readEvent.EventNumber == stream position 2
		// assert readEvent.ID == events[2].EventID
		require.EqualValues(t, 2, readEvent.GetOriginalEvent().EventNumber)
		require.Equal(t, events[2].EventID, readEvent.GetOriginalEvent().EventID)
	})
}

func Test_PersistentSubscription_ToExistingStream(t *testing.T) {
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClient(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	t.Run("StartFromBeginning_AndEventsInIt", func(t *testing.T) {
		// create 10 events
		events := testCreateEvents(10)

		streamID := "StartFromBeginning_AndEventsInIt"
		// append events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, Events);
		_, err := clientInstance.EventStreams().AppendToStream(context.Background(),
			streamID,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			events)
		require.NoError(t, err)
		// create persistent stream connection with Revision set to Start
		groupName := "Group StartFromBeginning_AndEventsInIt"
		request := persistent.CreateOrUpdateStreamRequest{
			StreamName: streamID,
			GroupName:  groupName,
			Revision:   persistent.StreamRevisionStart{},
			Settings:   persistent.DefaultRequestSettings,
		}
		err = clientInstance.PersistentSubscriptions().CreateStreamSubscription(
			context.Background(),
			request,
		)
		require.NoError(t, err)
		// read one event
		readConnectionClient, err := clientInstance.PersistentSubscriptions().
			SubscribeToStreamSync(context.Background(), 10, groupName, streamID)
		require.NoError(t, err)

		readEvent := readConnectionClient.Recv().EventAppeared
		require.NoError(t, err)
		require.NotNil(t, readEvent)

		// assert Event Number == stream Start
		// assert Event.ID == first event ID (readEvent.EventID == events[0].EventID)
		require.EqualValues(t, 0, readEvent.GetOriginalEvent().EventNumber)
		require.Equal(t, events[0].EventID, readEvent.GetOriginalEvent().EventID)
	})

	t.Run("StartFromEnd_EventsInItAndAppendEventsAfterwards", func(t *testing.T) {
		// create 11 events
		events := testCreateEvents(11)
		// append 10 events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, events[:10]);
		streamID := "StartFromEnd_EventsInItAndAppendEventsAfterwards"
		// append events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, Events);
		_, err := clientInstance.EventStreams().AppendToStream(context.Background(),
			streamID,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			events[:10])
		require.NoError(t, err)
		// create persistent stream connection with Revision set to End
		groupName := "Group StartFromEnd_EventsInItAndAppendEventsAfterwards"
		request := persistent.CreateOrUpdateStreamRequest{
			StreamName: streamID,
			GroupName:  groupName,
			Revision:   persistent.StreamRevisionEnd{},
			Settings:   persistent.DefaultRequestSettings,
		}
		err = clientInstance.PersistentSubscriptions().CreateStreamSubscription(
			context.Background(),
			request,
		)
		require.NoError(t, err)

		// append 1 event to StreamsClient.AppendToStreamAsync(Stream, new StreamRevision(9), event[10])
		_, err = clientInstance.EventStreams().AppendToStream(context.Background(),
			streamID,
			event_streams.AppendRequestExpectedStreamRevision{Revision: 9},
			events[10:])
		require.NoError(t, err)

		// read one event
		readConnectionClient, err := clientInstance.PersistentSubscriptions().
			SubscribeToStreamSync(context.Background(), 10, groupName, streamID)
		require.NoError(t, err)

		readEvent := readConnectionClient.Recv().EventAppeared
		require.NoError(t, err)
		require.NotNil(t, readEvent)
		// assert readEvent.EventNumber == stream position 10
		// assert readEvent.ID == events[10].EventID
		require.EqualValues(t, 10, readEvent.GetOriginalEvent().EventNumber)
		require.Equal(t, events[10].EventID, readEvent.GetOriginalEvent().EventID)
	})

	t.Run("StartFromEnd_EventsInIt", func(t *testing.T) {
		// create 10 events
		events := testCreateEvents(10)
		// append 10 events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, events[:10]);
		streamID := "StartFromEnd_EventsInIt"
		// append events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, Events);
		_, err := clientInstance.EventStreams().AppendToStream(context.Background(),
			streamID,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			events[:10])
		require.NoError(t, err)
		// create persistent stream connection with position set to End
		groupName := "Group StartFromEnd_EventsInIt"
		request := persistent.CreateOrUpdateStreamRequest{
			StreamName: streamID,
			GroupName:  groupName,
			Revision:   persistent.StreamRevisionEnd{},
			Settings:   persistent.DefaultRequestSettings,
		}
		err = clientInstance.PersistentSubscriptions().CreateStreamSubscription(
			context.Background(),
			request,
		)
		require.NoError(t, err)

		// reading one event after 10 seconds timeout will return no events
		ctx, cancelFunc := context.WithTimeout(context.Background(), 10*time.Second)
		readConnectionClient, err := clientInstance.PersistentSubscriptions().
			SubscribeToStreamSync(ctx, 10, groupName, streamID)
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
	})

	t.Run("StartFrom10_EventsInItAppendEventsAfterwards", func(t *testing.T) {
		// create 11 events
		events := testCreateEvents(11)

		// append 10 events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, events[:10]);
		streamID := "StartFrom10_EventsInItAppendEventsAfterwards"
		_, err := clientInstance.EventStreams().AppendToStream(context.Background(),
			streamID,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			events[:10])
		require.NoError(t, err)

		// create persistent stream connection with start position set to Position(10)
		groupName := "Group StartFrom10_EventsInItAppendEventsAfterwards"
		request := persistent.CreateOrUpdateStreamRequest{
			StreamName: streamID,
			GroupName:  groupName,
			Revision:   persistent.StreamRevision{Revision: 10},
			Settings:   persistent.DefaultRequestSettings,
		}
		err = clientInstance.PersistentSubscriptions().CreateStreamSubscription(
			context.Background(),
			request,
		)
		require.NoError(t, err)

		// append 1 event to StreamsClient.AppendToStreamAsync(Stream, StreamRevision(9), events[10:)
		_, err = clientInstance.EventStreams().AppendToStream(
			context.Background(),
			streamID,
			event_streams.AppendRequestExpectedStreamRevision{Revision: 9},
			events[10:])
		require.NoError(t, err)

		// read one event
		readConnectionClient, err := clientInstance.PersistentSubscriptions().
			SubscribeToStreamSync(context.Background(), 10, groupName, streamID)
		require.NoError(t, err)
		readEvent := readConnectionClient.Recv().EventAppeared
		require.NoError(t, err)
		require.NotNil(t, readEvent)

		// assert readEvent.EventNumber == stream position 10
		// assert readEvent.ID == events[10].EventID
		require.EqualValues(t, 10, readEvent.GetOriginalEvent().EventNumber)
		require.Equal(t, events[10].EventID, readEvent.GetOriginalEvent().EventID)
	})

	t.Run("StartFrom4_EventsInIt", func(t *testing.T) {
		// create 11 events
		events := testCreateEvents(11)

		// append 10 events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, events[:10]);
		streamID := "StartFrom4_EventsInIt"
		_, err := clientInstance.EventStreams().AppendToStream(
			context.Background(),
			streamID,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			events[:10])
		require.NoError(t, err)

		// create persistent stream connection with start position set to Position(4)
		groupName := "Group StartFrom4_EventsInIt"
		request := persistent.CreateOrUpdateStreamRequest{
			StreamName: streamID,
			GroupName:  groupName,
			Revision:   persistent.StreamRevision{Revision: 4},
			Settings:   persistent.DefaultRequestSettings,
		}
		err = clientInstance.PersistentSubscriptions().CreateStreamSubscription(
			context.Background(),
			request,
		)
		require.NoError(t, err)

		// append 1 event to StreamsClient.AppendToStreamAsync(Stream, StreamRevision(9), events)
		_, err = clientInstance.EventStreams().AppendToStream(
			context.Background(),
			streamID,
			event_streams.AppendRequestExpectedStreamRevision{Revision: 9},
			events[10:])
		require.NoError(t, err)

		// read one event
		readConnectionClient, err := clientInstance.PersistentSubscriptions().
			SubscribeToStreamSync(context.Background(), 10, groupName, streamID)
		require.NoError(t, err)
		readEvent := readConnectionClient.Recv().EventAppeared
		require.NoError(t, err)
		require.NotNil(t, readEvent)

		// assert readEvent.EventNumber == stream position 4
		// assert readEvent.ID == events[4].EventID
		require.EqualValues(t, 4, readEvent.GetOriginalEvent().EventNumber)
		require.Equal(t, events[4].EventID, readEvent.GetOriginalEvent().EventID)
	})

	t.Run("StartFromHigherRevisionThenEventsInStream_EventsInItAppendEventsAfterwards", func(t *testing.T) {
		// create 12 events
		events := testCreateEvents(12)

		// append 11 events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, events[:11]);
		// append 10 events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, events[:10]);
		streamID := "StartFromHigherRevisionThenEventsInStream_EventsInItAppendEventsAfterwards"
		_, err := clientInstance.EventStreams().AppendToStream(
			context.Background(),
			streamID,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			events[:11])
		require.NoError(t, err)

		// create persistent stream connection with start position set to Position(11)
		groupName := "Group StartFromHigherRevisionThenEventsInStream_EventsInItAppendEventsAfterwards"
		request := persistent.CreateOrUpdateStreamRequest{
			StreamName: streamID,
			GroupName:  groupName,
			Revision:   persistent.StreamRevision{Revision: 11},
			Settings:   persistent.DefaultRequestSettings,
		}
		err = clientInstance.PersistentSubscriptions().CreateStreamSubscription(
			context.Background(),
			request,
		)
		require.NoError(t, err)

		// append event to StreamsClient.AppendToStreamAsync(Stream, StreamRevision(10), events[11:])
		_, err = clientInstance.EventStreams().AppendToStream(
			context.Background(),
			streamID,
			event_streams.AppendRequestExpectedStreamRevision{Revision: 10},
			events[11:])
		require.NoError(t, err)

		// read one event
		readConnectionClient, err := clientInstance.PersistentSubscriptions().
			SubscribeToStreamSync(context.Background(), 10, groupName, streamID)
		require.NoError(t, err)
		readEvent := readConnectionClient.Recv().EventAppeared
		require.NoError(t, err)
		require.NotNil(t, readEvent)

		// assert readEvent.EventNumber == stream position 11
		// assert readEvent.ID == events[11].EventID
		require.EqualValues(t, 11, readEvent.GetOriginalEvent().EventNumber)
		require.Equal(t, events[11].EventID, readEvent.GetOriginalEvent().EventID)
	})
}
