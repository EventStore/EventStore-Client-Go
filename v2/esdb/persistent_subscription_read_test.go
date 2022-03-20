package esdb_test

import (
	"context"
	"testing"
	"time"

	esdb2 "github.com/EventStore/EventStore-Client-Go/v2/esdb"
	"github.com/stretchr/testify/require"
)

func PersistentSubReadTests(t *testing.T, emptyDBClient *esdb2.Client) {
	t.Run("PersistentSubReadTests", func(t *testing.T) {
		t.Run("ReadExistingStream_AckToReceiveNewEvents", persistentSubscription_ReadExistingStream_AckToReceiveNewEvents(emptyDBClient))
		t.Run("ToExistingStream_StartFromBeginning_AndEventsInIt", persistentSubscription_ToExistingStream_StartFromBeginning_AndEventsInIt(emptyDBClient))
		t.Run("ToNonExistingStream_StartFromBeginning_AppendEventsAfterwards", persistentSubscription_ToNonExistingStream_StartFromBeginning_AppendEventsAfterwards(emptyDBClient))
		t.Run("ToExistingStream_StartFromEnd_EventsInItAndAppendEventsAfterwards", persistentSubscription_ToExistingStream_StartFromEnd_EventsInItAndAppendEventsAfterwards(emptyDBClient))
		t.Run("ToExistingStream_StartFromEnd_EventsInIt", persistentSubscription_ToExistingStream_StartFromEnd_EventsInIt(emptyDBClient))
		t.Run("ToNonExistingStream_StartFromTwo_AppendEventsAfterwards", persistentSubscription_ToNonExistingStream_StartFromTwo_AppendEventsAfterwards(emptyDBClient))
		t.Run("ToExistingStream_StartFrom10_EventsInItAppendEventsAfterwards", persistentSubscription_ToExistingStream_StartFrom10_EventsInItAppendEventsAfterwards(emptyDBClient))
		t.Run("ToExistingStream_StartFrom4_EventsInIt", persistentSubscription_ToExistingStream_StartFrom4_EventsInIt(emptyDBClient))
		t.Run("ToExistingStream_StartFromHigherRevisionThenEventsInStream_EventsInItAppendEventsAfterwards", persistentSubscription_ToExistingStream_StartFromHigherRevisionThenEventsInStream_EventsInItAppendEventsAfterwards(emptyDBClient))
		t.Run("ReadExistingStream_NackToReceiveNewEvents", persistentSubscription_ReadExistingStream_NackToReceiveNewEvents(emptyDBClient))
		t.Run("persistentSubscriptionToAll_Read", persistentSubscriptionToAll_Read(emptyDBClient))
	})
}

func persistentSubscription_ReadExistingStream_AckToReceiveNewEvents(clientInstance *esdb2.Client) TestCall {
	return func(t *testing.T) {
		streamID := NAME_GENERATOR.Generate()
		firstEvent := createTestEvent()
		secondEvent := createTestEvent()
		thirdEvent := createTestEvent()
		events := []esdb2.EventData{firstEvent, secondEvent, thirdEvent}
		pushEventsToStream(t, clientInstance, streamID, events)

		groupName := "Group 1"
		err := clientInstance.CreatePersistentSubscription(
			context.Background(),
			streamID,
			groupName,
			esdb2.PersistentStreamSubscriptionOptions{
				StartFrom: esdb2.Start{},
			},
		)
		require.NoError(t, err)

		readConnectionClient, err := clientInstance.SubscribeToPersistentSubscription(
			context.Background(), streamID, groupName, esdb2.SubscribeToPersistentSubscriptionOptions{
				BufferSize: 2,
			})
		require.NoError(t, err)

		firstReadEvent := readConnectionClient.Recv().EventAppeared.Event
		require.NoError(t, err)
		require.NotNil(t, firstReadEvent)

		secondReadEvent := readConnectionClient.Recv().EventAppeared.Event
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
}

func persistentSubscription_ToExistingStream_StartFromBeginning_AndEventsInIt(clientInstance *esdb2.Client) TestCall {
	return func(t *testing.T) {
		// create 10 events
		events := testCreateEvents(10)

		streamID := NAME_GENERATOR.Generate()
		// append events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, Events);
		opts := esdb2.AppendToStreamOptions{
			ExpectedRevision: esdb2.NoStream{},
		}

		_, err := clientInstance.AppendToStream(context.Background(), streamID, opts, events...)
		require.NoError(t, err)
		// create persistent stream connection with StreamRevision set to Start
		groupName := "Group 1"
		err = clientInstance.CreatePersistentSubscription(
			context.Background(),
			streamID,
			groupName,
			esdb2.PersistentStreamSubscriptionOptions{
				StartFrom: esdb2.Start{},
			},
		)
		require.NoError(t, err)
		// read one event
		readConnectionClient, err := clientInstance.SubscribeToPersistentSubscription(
			context.Background(), streamID, groupName, esdb2.SubscribeToPersistentSubscriptionOptions{})
		require.NoError(t, err)

		readEvent := readConnectionClient.Recv().EventAppeared.Event
		require.NoError(t, err)
		require.NotNil(t, readEvent)

		// assert Event Number == stream Start
		// assert Event.ID == first event ID (readEvent.EventID == events[0].EventID)
		require.EqualValues(t, 0, readEvent.OriginalEvent().EventNumber)
		require.Equal(t, events[0].EventID, readEvent.OriginalEvent().EventID)
	}
}

func persistentSubscription_ToNonExistingStream_StartFromBeginning_AppendEventsAfterwards(clientInstance *esdb2.Client) TestCall {
	return func(t *testing.T) {
		// create 10 events
		events := testCreateEvents(10)

		// create persistent stream connection with StreamRevision set to Start
		streamID := NAME_GENERATOR.Generate()
		groupName := "Group 1"
		err := clientInstance.CreatePersistentSubscription(
			context.Background(),
			streamID,
			groupName,
			esdb2.PersistentStreamSubscriptionOptions{
				StartFrom: esdb2.Start{},
			},
		)
		require.NoError(t, err)
		// append events to StreamsClient.AppendToStreamAsync(Stream, stream_revision.StreamRevisionNoStream, Events);
		opts := esdb2.AppendToStreamOptions{
			ExpectedRevision: esdb2.NoStream{},
		}
		_, err = clientInstance.AppendToStream(context.Background(), streamID, opts, events...)
		require.NoError(t, err)
		// read one event

		readConnectionClient, err := clientInstance.SubscribeToPersistentSubscription(
			context.Background(), streamID, groupName, esdb2.SubscribeToPersistentSubscriptionOptions{})
		require.NoError(t, err)

		readEvent := readConnectionClient.Recv().EventAppeared.Event
		require.NoError(t, err)
		require.NotNil(t, readEvent)
		// assert Event Number == stream Start
		// assert Event.ID == first event ID (readEvent.EventID == events[0].EventID)
		require.EqualValues(t, 0, readEvent.OriginalEvent().EventNumber)
		require.Equal(t, events[0].EventID, readEvent.OriginalEvent().EventID)
	}
}

func persistentSubscription_ToExistingStream_StartFromEnd_EventsInItAndAppendEventsAfterwards(clientInstance *esdb2.Client) TestCall {
	return func(t *testing.T) {
		// create 11 events
		events := testCreateEvents(11)
		// append 10 events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, events[:10]);
		streamID := NAME_GENERATOR.Generate()
		// append events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, Events);
		opts := esdb2.AppendToStreamOptions{
			ExpectedRevision: esdb2.NoStream{},
		}
		_, err := clientInstance.AppendToStream(context.Background(), streamID, opts, events[:10]...)
		require.NoError(t, err)
		// create persistent stream connection with StreamRevision set to End
		groupName := "Group 1"
		err = clientInstance.CreatePersistentSubscription(
			context.Background(),
			streamID,
			groupName,
			esdb2.PersistentStreamSubscriptionOptions{
				StartFrom: esdb2.End{},
			},
		)
		require.NoError(t, err)

		// append 1 event to StreamsClient.AppendToStreamAsync(Stream, new StreamRevision(9), event[10])
		opts.ExpectedRevision = esdb2.Revision(9)
		_, err = clientInstance.AppendToStream(context.Background(), streamID, opts, events[10:]...)
		require.NoError(t, err)

		// read one event
		readConnectionClient, err := clientInstance.SubscribeToPersistentSubscription(
			context.Background(), streamID, groupName, esdb2.SubscribeToPersistentSubscriptionOptions{})
		require.NoError(t, err)

		readEvent := readConnectionClient.Recv().EventAppeared.Event
		require.NoError(t, err)
		require.NotNil(t, readEvent)
		// assert readEvent.EventNumber == stream StartFrom 10
		// assert readEvent.ID == events[10].EventID
		require.EqualValues(t, 10, readEvent.OriginalEvent().EventNumber)
		require.Equal(t, events[10].EventID, readEvent.OriginalEvent().EventID)
	}
}

func persistentSubscription_ToExistingStream_StartFromEnd_EventsInIt(clientInstance *esdb2.Client) TestCall {
	return func(t *testing.T) {
		// create 10 events
		events := testCreateEvents(10)
		// append 10 events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, events[:10]);
		streamID := NAME_GENERATOR.Generate()
		// append events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, Events);
		opts := esdb2.AppendToStreamOptions{
			ExpectedRevision: esdb2.NoStream{},
		}

		_, err := clientInstance.AppendToStream(context.Background(), streamID, opts, events[:10]...)
		require.NoError(t, err)
		// create persistent stream connection with StartFrom set to End
		groupName := "Group 1"
		err = clientInstance.CreatePersistentSubscription(
			context.Background(),
			streamID,
			groupName,
			esdb2.PersistentStreamSubscriptionOptions{
				StartFrom: esdb2.End{},
			},
		)
		require.NoError(t, err)

		ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
		readConnectionClient, err := clientInstance.SubscribeToPersistentSubscription(
			ctx, streamID, groupName, esdb2.SubscribeToPersistentSubscriptionOptions{})
		require.NoError(t, err)

		doneChannel := make(chan struct{})
		go func() {
			event := readConnectionClient.Recv()

			if event.EventAppeared != nil {
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
}

func persistentSubscription_ToNonExistingStream_StartFromTwo_AppendEventsAfterwards(clientInstance *esdb2.Client) TestCall {
	return func(t *testing.T) {
		// create 3 events
		events := testCreateEvents(4)
		// create persistent stream connection with StartFrom set to Position(2)
		streamID := NAME_GENERATOR.Generate()
		groupName := "Group 1"
		err := clientInstance.CreatePersistentSubscription(
			context.Background(),
			streamID,
			groupName,
			esdb2.PersistentStreamSubscriptionOptions{
				StartFrom: esdb2.Revision(2),
			},
		)
		require.NoError(t, err)
		// append 3 event to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, events)
		opts := esdb2.AppendToStreamOptions{
			ExpectedRevision: esdb2.NoStream{},
		}
		_, err = clientInstance.AppendToStream(context.Background(), streamID, opts, events...)
		require.NoError(t, err)
		// read one event
		readConnectionClient, err := clientInstance.SubscribeToPersistentSubscription(
			context.Background(), streamID, groupName, esdb2.SubscribeToPersistentSubscriptionOptions{})
		require.NoError(t, err)
		readEvent := readConnectionClient.Recv().EventAppeared.Event
		require.NoError(t, err)
		require.NotNil(t, readEvent)

		// assert readEvent.EventNumber == stream StartFrom 2
		// assert readEvent.ID == events[2].EventID
		require.EqualValues(t, 2, readEvent.OriginalEvent().EventNumber)
		require.Equal(t, events[2].EventID, readEvent.OriginalEvent().EventID)
	}
}

func persistentSubscription_ToExistingStream_StartFrom10_EventsInItAppendEventsAfterwards(clientInstance *esdb2.Client) TestCall {
	return func(t *testing.T) {
		// create 11 events
		events := testCreateEvents(11)

		// append 10 events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, events[:10]);
		opts := esdb2.AppendToStreamOptions{
			ExpectedRevision: esdb2.NoStream{},
		}
		streamID := NAME_GENERATOR.Generate()
		_, err := clientInstance.AppendToStream(context.Background(), streamID, opts, events[:10]...)
		require.NoError(t, err)

		// create persistent stream connection with start StartFrom set to Position(10)
		groupName := "Group 1"
		err = clientInstance.CreatePersistentSubscription(
			context.Background(),
			streamID,
			groupName,
			esdb2.PersistentStreamSubscriptionOptions{
				StartFrom: esdb2.Revision(10),
			},
		)
		require.NoError(t, err)

		// append 1 event to StreamsClient.AppendToStreamAsync(Stream, StreamRevision(9), events[10:)
		opts = esdb2.AppendToStreamOptions{
			ExpectedRevision: esdb2.Revision(9),
		}
		_, err = clientInstance.AppendToStream(context.Background(), streamID, opts, events[10:]...)
		require.NoError(t, err)

		// read one event
		readConnectionClient, err := clientInstance.SubscribeToPersistentSubscription(
			context.Background(), streamID, groupName, esdb2.SubscribeToPersistentSubscriptionOptions{})
		require.NoError(t, err)
		readEvent := readConnectionClient.Recv().EventAppeared.Event
		require.NoError(t, err)
		require.NotNil(t, readEvent)

		// assert readEvent.EventNumber == stream StartFrom 10
		// assert readEvent.ID == events[10].EventID
		require.EqualValues(t, 10, readEvent.OriginalEvent().EventNumber)
		require.Equal(t, events[10].EventID, readEvent.OriginalEvent().EventID)
	}
}

func persistentSubscription_ToExistingStream_StartFrom4_EventsInIt(clientInstance *esdb2.Client) TestCall {
	return func(t *testing.T) {
		// create 11 events
		events := testCreateEvents(11)

		// append 10 events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, events[:10]);
		opts := esdb2.AppendToStreamOptions{
			ExpectedRevision: esdb2.NoStream{},
		}
		streamID := NAME_GENERATOR.Generate()
		_, err := clientInstance.AppendToStream(context.Background(), streamID, opts, events[:10]...)
		require.NoError(t, err)

		// create persistent stream connection with start StartFrom set to Position(4)
		groupName := "Group 1"

		err = clientInstance.CreatePersistentSubscription(
			context.Background(),
			streamID,
			groupName,
			esdb2.PersistentStreamSubscriptionOptions{
				StartFrom: esdb2.Revision(4),
			},
		)
		require.NoError(t, err)

		// append 1 event to StreamsClient.AppendToStreamAsync(Stream, StreamRevision(9), events)
		opts = esdb2.AppendToStreamOptions{
			ExpectedRevision: esdb2.Revision(9),
		}
		_, err = clientInstance.AppendToStream(context.Background(), streamID, opts, events[10:]...)
		require.NoError(t, err)

		// read one event
		readConnectionClient, err := clientInstance.SubscribeToPersistentSubscription(
			context.Background(), streamID, groupName, esdb2.SubscribeToPersistentSubscriptionOptions{})
		require.NoError(t, err)
		readEvent := readConnectionClient.Recv().EventAppeared.Event
		require.NoError(t, err)
		require.NotNil(t, readEvent)

		// assert readEvent.EventNumber == stream StartFrom 4
		// assert readEvent.ID == events[4].EventID
		require.EqualValues(t, 4, readEvent.OriginalEvent().EventNumber)
		require.Equal(t, events[4].EventID, readEvent.OriginalEvent().EventID)
	}
}

func persistentSubscription_ToExistingStream_StartFromHigherRevisionThenEventsInStream_EventsInItAppendEventsAfterwards(clientInstance *esdb2.Client) TestCall {
	return func(t *testing.T) {
		// create 12 events
		events := testCreateEvents(12)

		// append 11 events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, events[:11]);
		// append 10 events to StreamsClient.AppendToStreamAsync(Stream, StreamState.NoStream, events[:10]);
		streamID := NAME_GENERATOR.Generate()
		opts := esdb2.AppendToStreamOptions{
			ExpectedRevision: esdb2.NoStream{},
		}
		_, err := clientInstance.AppendToStream(context.Background(), streamID, opts, events[:11]...)
		require.NoError(t, err)

		// create persistent stream connection with start StartFrom set to Position(11)
		groupName := "Group 1"
		err = clientInstance.CreatePersistentSubscription(
			context.Background(),
			streamID,
			groupName,
			esdb2.PersistentStreamSubscriptionOptions{
				StartFrom: esdb2.Revision(11),
			},
		)
		require.NoError(t, err)

		// append event to StreamsClient.AppendToStreamAsync(Stream, StreamRevision(10), events[11:])
		opts = esdb2.AppendToStreamOptions{
			ExpectedRevision: esdb2.Revision(10),
		}

		_, err = clientInstance.AppendToStream(context.Background(), streamID, opts, events[11])
		require.NoError(t, err)

		// read one event
		readConnectionClient, err := clientInstance.SubscribeToPersistentSubscription(
			context.Background(), streamID, groupName, esdb2.SubscribeToPersistentSubscriptionOptions{})
		require.NoError(t, err)
		readEvent := readConnectionClient.Recv().EventAppeared.Event
		require.NoError(t, err)
		require.NotNil(t, readEvent)

		// assert readEvent.EventNumber == stream StartFrom 11
		// assert readEvent.ID == events[11].EventID
		require.EqualValues(t, 11, readEvent.OriginalEvent().EventNumber)
		require.Equal(t, events[11].EventID, readEvent.OriginalEvent().EventID)
	}
}

func persistentSubscription_ReadExistingStream_NackToReceiveNewEvents(clientInstance *esdb2.Client) TestCall {
	return func(t *testing.T) {
		streamID := NAME_GENERATOR.Generate()
		firstEvent := createTestEvent()
		secondEvent := createTestEvent()
		thirdEvent := createTestEvent()
		events := []esdb2.EventData{firstEvent, secondEvent, thirdEvent}
		pushEventsToStream(t, clientInstance, streamID, events)

		groupName := "Group 1"
		err := clientInstance.CreatePersistentSubscription(
			context.Background(),
			streamID,
			groupName,
			esdb2.PersistentStreamSubscriptionOptions{
				StartFrom: esdb2.Start{},
			},
		)

		readConnectionClient, err := clientInstance.SubscribeToPersistentSubscription(
			context.Background(), streamID, groupName, esdb2.SubscribeToPersistentSubscriptionOptions{
				BufferSize: 2,
			})
		require.NoError(t, err)

		firstReadEvent := readConnectionClient.Recv().EventAppeared.Event
		require.NoError(t, err)
		require.NotNil(t, firstReadEvent)

		secondReadEvent := readConnectionClient.Recv().EventAppeared.Event
		require.NoError(t, err)
		require.NotNil(t, secondReadEvent)

		// since buffer size is two, after reading two outstanding messages
		// we must acknowledge a message in order to receive third one
		err = readConnectionClient.Nack("test reason", esdb2.Nack_Park, firstReadEvent)
		require.NoError(t, err)

		thirdReadEvent := readConnectionClient.Recv()
		require.NoError(t, err)
		require.NotNil(t, thirdReadEvent)
	}
}

func persistentSubscriptionToAll_Read(clientInstance *esdb2.Client) TestCall {
	return func(t *testing.T) {
		groupName := "Group 1"
		err := clientInstance.CreatePersistentSubscriptionToAll(
			context.Background(),
			groupName,
			esdb2.PersistentAllSubscriptionOptions{
				StartFrom: esdb2.Start{},
			},
		)

		if err, ok := esdb2.FromError(err); !ok {
			if err.Code() == esdb2.ErrorUnsupportedFeature && IsESDBVersion20() {
				t.Skip()
			}
		}

		require.NoError(t, err)

		readConnectionClient, err := clientInstance.SubscribeToPersistentSubscriptionToAll(
			context.Background(), groupName, esdb2.SubscribeToPersistentSubscriptionOptions{
				BufferSize: 2,
			},
		)
		require.NoError(t, err)

		firstReadEvent := readConnectionClient.Recv().EventAppeared.Event
		require.NoError(t, err)
		require.NotNil(t, firstReadEvent)

		secondReadEvent := readConnectionClient.Recv().EventAppeared.Event
		require.NoError(t, err)
		require.NotNil(t, secondReadEvent)

		// since buffer size is two, after reading two outstanding messages
		// we must acknowledge a message in order to receive third one
		err = readConnectionClient.Ack(firstReadEvent)
		require.NoError(t, err)

		thirdReadEvent := readConnectionClient.Recv()
		require.NoError(t, err)
		require.NotNil(t, thirdReadEvent)
		err = readConnectionClient.Ack(thirdReadEvent.EventAppeared.Event)
		require.NoError(t, err)
	}
}

func testCreateEvents(count uint32) []esdb2.EventData {
	result := make([]esdb2.EventData, count)
	var i uint32 = 0
	for ; i < count; i++ {
		result[i] = createTestEvent()
	}
	return result
}
