package client_test

import (
	"context"
	"testing"

	"github.com/EventStore/EventStore-Client-Go/errors"
	"github.com/EventStore/EventStore-Client-Go/event_streams"
	"github.com/stretchr/testify/require"

	"github.com/gofrs/uuid"
)

func createTestEvent() event_streams.ProposedEvent {
	return event_streams.ProposedEvent{
		EventID:      uuid.Must(uuid.NewV4()),
		EventType:    "TestEvent",
		ContentType:  "application/octet-stream",
		UserMetadata: []byte{0xd, 0xe, 0xa, 0xd},
		Data:         []byte{0xb, 0xe, 0xe, 0xf},
	}
}

func Test_AppendingZeroEvents_ExpectedStreamStateAny(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()
	client := CreateTestClient(container, t)
	defer func() {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}()

	expectedStreamRevision := event_streams.AppendRequestExpectedStreamRevisionAny{}
	streamName := "test_any"

	iterations := 2
	for ; iterations > 0; iterations-- {
		writeResult, err := client.AppendToStream(context.Background(),
			streamName,
			expectedStreamRevision,
			[]event_streams.ProposedEvent{})
		require.NoError(t, err)
		success, isSuccess := writeResult.GetSuccess()
		require.True(t, isSuccess)
		require.True(t, success.GetCurrentRevisionNoStream())
	}

	_, err := client.ReadStreamEvents(context.Background(),
		streamName,
		event_streams.ReadRequestDirectionForward,
		event_streams.ReadRequestOptionsStreamRevisionStart{},
		2,
		false)
	require.Equal(t, err.Code(), event_streams.StreamNotFoundErr)
}

func Test_AppendingZeroEvents_expectedStreamStateNoStream(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()
	client := CreateTestClient(container, t)
	defer func() {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}()

	expectedStreamRevision := event_streams.AppendRequestExpectedStreamRevisionNoStream{}
	streamName := "test_no_stream"

	iterations := 2
	for ; iterations > 0; iterations-- {
		writeResult, err := client.AppendToStream(context.Background(),
			streamName,
			expectedStreamRevision,
			[]event_streams.ProposedEvent{})
		require.NoError(t, err)
		success, isSuccess := writeResult.GetSuccess()
		require.True(t, isSuccess)
		require.True(t, success.GetCurrentRevisionNoStream())
	}

	_, err := client.ReadStreamEvents(context.Background(),
		streamName,
		event_streams.ReadRequestDirectionForward,
		event_streams.ReadRequestOptionsStreamRevisionStart{},
		2,
		false)
	require.Equal(t, err.Code(), errors.StreamNotFoundErr)
}

func Test_CreateStreamIfDoesNotExist_expectedStreamAny(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()
	client := CreateTestClient(container, t)
	defer func() {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}()

	expectedStreamRevision := event_streams.AppendRequestExpectedStreamRevisionAny{}
	streamName := "stream_any"

	testEvent := createTestEvent()

	writeResult, err := client.AppendToStream(context.Background(),
		streamName,
		expectedStreamRevision,
		[]event_streams.ProposedEvent{testEvent})
	require.NoError(t, err)
	success, _ := writeResult.GetSuccess()
	require.EqualValues(t, 0, success.GetCurrentRevision())

	events, err := client.ReadStreamEvents(context.Background(),
		streamName,
		event_streams.ReadRequestDirectionForward,
		event_streams.ReadRequestOptionsStreamRevisionStart{},
		2,
		false)
	require.NoError(t, err)
	require.Len(t, events, 1)
}

func Test_CreateStreamIfDoesNotExist_expectedStreamNoStream(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()
	client := CreateTestClient(container, t)
	defer func() {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}()

	expectedStreamRevision := event_streams.AppendRequestExpectedStreamRevisionNoStream{}
	streamName := "stream_no_stream"

	testEvent := createTestEvent()

	writeResult, err := client.AppendToStream(context.Background(),
		streamName,
		expectedStreamRevision,
		[]event_streams.ProposedEvent{testEvent})
	require.NoError(t, err)
	success, _ := writeResult.GetSuccess()
	require.EqualValues(t, 0, success.GetCurrentRevision())

	events, err := client.ReadStreamEvents(context.Background(),
		streamName,
		event_streams.ReadRequestDirectionForward,
		event_streams.ReadRequestOptionsStreamRevisionStart{},
		2,
		false)
	require.NoError(t, err)
	require.Len(t, events, 1)
}

func Test_MultipleIdempotentWrites_ExpectedStreamAny(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()
	client := CreateTestClient(container, t)
	defer func() {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}()

	expectedStreamRevision := event_streams.AppendRequestExpectedStreamRevisionAny{}
	streamName := "stream_any"
	events := testCreateEvents(4)

	writeResult, err := client.AppendToStream(context.Background(),
		streamName,
		expectedStreamRevision,
		events)
	require.NoError(t, err)
	success, _ := writeResult.GetSuccess()
	require.EqualValues(t, 3, success.GetCurrentRevision())

	writeResult, err = client.AppendToStream(context.Background(),
		streamName,
		expectedStreamRevision,
		events)
	require.NoError(t, err)
	success, _ = writeResult.GetSuccess()
	require.EqualValues(t, 3, success.GetCurrentRevision())
}

func Test_MultipleIdempotentWritesWithSameIdBugCase_ExpectedStreamAny(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()
	client := CreateTestClient(container, t)
	defer func() {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}()

	expectedStreamRevision := event_streams.AppendRequestExpectedStreamRevisionAny{}
	streamName := "stream_any"
	event := createTestEvent()
	events := []event_streams.ProposedEvent{event, event, event, event, event, event}

	writeResult, err := client.AppendToStream(context.Background(),
		streamName,
		expectedStreamRevision,
		events)
	require.NoError(t, err)
	success, _ := writeResult.GetSuccess()
	require.EqualValues(t, 5, success.GetCurrentRevision())
}

func Test_MultipleWriteEventsWithSameIds_ExpectedVersionAny_NextExpectedVersionIsUnreliable(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()
	client := CreateTestClient(container, t)
	defer func() {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}()

	expectedStreamRevision := event_streams.AppendRequestExpectedStreamRevisionAny{}
	streamName := "stream_any"
	event := createTestEvent()
	events := []event_streams.ProposedEvent{event, event, event, event, event, event}

	writeResult, err := client.AppendToStream(context.Background(),
		streamName,
		expectedStreamRevision,
		events)
	require.NoError(t, err)
	success, _ := writeResult.GetSuccess()
	require.EqualValues(t, 5, success.GetCurrentRevision())

	writeResult, err = client.AppendToStream(context.Background(),
		streamName,
		expectedStreamRevision,
		events)
	require.NoError(t, err)
	success, _ = writeResult.GetSuccess()
	require.EqualValues(t, 0, success.GetCurrentRevision())
}

func Test_MultipleWriteEventsWithSameIds_ExpectedVersionNoStream_NextExpectedVersionIsUnreliable(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()
	client := CreateTestClient(container, t)
	defer func() {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}()

	expectedStreamRevision := event_streams.AppendRequestExpectedStreamRevisionNoStream{}
	streamName := "stream_no_stream"
	event := createTestEvent()
	events := []event_streams.ProposedEvent{event, event, event, event, event, event}

	writeResult, err := client.AppendToStream(context.Background(),
		streamName,
		expectedStreamRevision,
		events)
	require.NoError(t, err)
	success, _ := writeResult.GetSuccess()
	require.EqualValues(t, 5, success.GetCurrentRevision())

	writeResult, err = client.AppendToStream(context.Background(),
		streamName,
		expectedStreamRevision,
		events)
	require.NoError(t, err)
	success, _ = writeResult.GetSuccess()
	require.EqualValues(t, 5, success.GetCurrentRevision())
}

func Test_ReturnsPositionWhenWriting(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()
	client := CreateTestClient(container, t)
	defer func() {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}()

	streamName := "stream_no_stream"
	event := createTestEvent()
	expectedStreamRevision := event_streams.AppendRequestExpectedStreamRevisionNoStream{}
	writeResult, _ := client.AppendToStream(context.Background(),
		streamName,
		expectedStreamRevision,
		[]event_streams.ProposedEvent{event})

	writeSuccess, _ := writeResult.GetSuccess()
	position, _ := writeSuccess.GetPosition()
	require.Greater(t, position.PreparePosition, uint64(0))
	require.Greater(t, position.CommitPosition, uint64(0))
}

func Test_WritingWithExpectedVersionNoStream_ToDeletedStream_StreamDeletedErr(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()
	client := CreateTestClient(container, t)
	defer func() {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}()

	streamName := "stream_no_stream"

	_, err := client.TombstoneStreamNoStreamRevision(context.Background(), streamName)
	require.NoError(t, err)

	event := createTestEvent()
	expectedStreamRevision := event_streams.AppendRequestExpectedStreamRevisionNoStream{}

	_, err = client.AppendToStream(context.Background(),
		streamName,
		expectedStreamRevision,
		[]event_streams.ProposedEvent{event})
	require.Equal(t, errors.StreamDeletedErr, err.Code())
}

func Test_WritingWithExpectedVersionAny_ToDeletedStream_StreamDeletedErr(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()
	client := CreateTestClient(container, t)
	defer func() {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}()

	streamName := "stream_no_stream"

	_, err := client.TombstoneStreamNoStreamRevision(context.Background(), streamName)
	require.NoError(t, err)

	event := createTestEvent()
	expectedStreamRevision := event_streams.AppendRequestExpectedStreamRevisionAny{}

	_, err = client.AppendToStream(context.Background(),
		streamName,
		expectedStreamRevision,
		[]event_streams.ProposedEvent{event})
	require.Equal(t, errors.StreamDeletedErr, err.Code())
}

func Test_WritingWithInvalidExpectedVersion_ToDeletedStream_StreamDeletedErr(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()
	client := CreateTestClient(container, t)
	defer func() {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}()

	streamName := "stream_no_stream"

	_, err := client.TombstoneStreamNoStreamRevision(context.Background(), streamName)
	require.NoError(t, err)

	event := createTestEvent()
	expectedStreamRevision := event_streams.AppendRequestExpectedStreamRevision{
		Revision: 5,
	}

	_, err = client.AppendToStream(context.Background(),
		streamName,
		expectedStreamRevision,
		[]event_streams.ProposedEvent{event})
	require.Equal(t, errors.StreamDeletedErr, err.Code())
}

func Test_append_with_correct_expected_version_to_existing_stream(t *testing.T) {
	//var stream = _fixture.GetStreamName();
	//
	//var writeResult = await _fixture.Client.AppendToStreamAsync(
	//	stream,
	//	StreamState.NoStream,
	//	_fixture.CreateTestEvents(1));
	//
	//writeResult = await _fixture.Client.AppendToStreamAsync(
	//	stream,
	//	writeResult.NextExpectedStreamRevision,
	//	_fixture.CreateTestEvents());
	//
	//Assert.Equal(new StreamRevision(1), writeResult.NextExpectedStreamRevision);
}

func Test_append_with_any_expected_version_to_existing_stream(t *testing.T) {
	//var stream = _fixture.GetStreamName();
	//
	//var writeResult = await _fixture.Client.AppendToStreamAsync(
	//	stream,
	//	StreamState.NoStream,
	//	_fixture.CreateTestEvents(1));
	//
	//Assert.Equal(new StreamRevision(0), writeResult.NextExpectedStreamRevision);
	//
	//writeResult = await _fixture.Client.AppendToStreamAsync(
	//	stream,
	//	StreamState.Any,
	//	_fixture.CreateTestEvents(1));
	//
	//Assert.Equal(new StreamRevision(1), writeResult.NextExpectedStreamRevision);
}

func Test_appending_with_wrong_expected_version_to_existing_stream_throws_wrong_expected_version(t *testing.T) {
	//var stream = _fixture.GetStreamName();
	//
	//await _fixture.Client.AppendToStreamAsync(stream, StreamState.NoStream, _fixture.CreateTestEvents());
	//
	//var ex = await Assert.ThrowsAsync<WrongExpectedVersionException>(
	//	() => _fixture.Client.AppendToStreamAsync(stream, new StreamRevision(999), _fixture.CreateTestEvents()));
	//Assert.Equal(new StreamRevision(0), ex.ActualStreamRevision);
	//Assert.Equal(new StreamRevision(999), ex.ExpectedStreamRevision);
}

func Test_appending_with_wrong_expected_version_to_existing_stream_returns_wrong_expected_version(t *testing.T) {
	//var stream = _fixture.GetStreamName();
	//
	//var writeResult = await _fixture.Client.AppendToStreamAsync(stream, new StreamRevision(1),
	//	_fixture.CreateTestEvents(), options => {
	//	options.ThrowOnAppendFailure = false;
	//});
	//
	//var wrongExpectedVersionResult = (WrongExpectedVersionResult)writeResult;
	//
	//Assert.Equal(new StreamRevision(1), wrongExpectedVersionResult.NextExpectedStreamRevision);
}

func Test_append_with_stream_exists_expected_version_to_existing_stream(t *testing.T) {
	//var stream = _fixture.GetStreamName();
	//
	//await _fixture.Client.AppendToStreamAsync(stream, StreamState.NoStream, _fixture.CreateTestEvents());
	//
	//await _fixture.Client.AppendToStreamAsync(stream, StreamState.StreamExists,
	//	_fixture.CreateTestEvents());
}

func Test_append_with_stream_exists_expected_version_to_stream_with_multiple_events(t *testing.T) {
	//var stream = _fixture.GetStreamName();
	//
	//for (var i = 0; i < 5; i++) {
	//	await _fixture.Client.AppendToStreamAsync(stream, StreamState.Any, _fixture.CreateTestEvents(1));
	//}
	//
	//await _fixture.Client.AppendToStreamAsync(
	//	stream,
	//	StreamState.StreamExists,
	//	_fixture.CreateTestEvents());
}

func Test_append_with_stream_exists_expected_version_if_metadata_stream_exists(t *testing.T) {
	//var stream = _fixture.GetStreamName();
	//
	//await _fixture.Client.SetStreamMetadataAsync(stream, StreamState.Any,
	//	new StreamMetadata(10, default));
	//
	//await _fixture.Client.AppendToStreamAsync(
	//stream,
	//StreamState.StreamExists,
	//_fixture.CreateTestEvents());
}

func Test_appending_with_stream_exists_expected_version_and_stream_does_not_exist_throws_wrong_expected_version(t *testing.T) {
	//var stream = _fixture.GetStreamName();
	//
	//var ex = await Assert.ThrowsAsync<WrongExpectedVersionException>(
	//	() => _fixture.Client.AppendToStreamAsync(stream, StreamState.StreamExists,
	//	_fixture.CreateTestEvents()));
	//
	//Assert.Equal(StreamRevision.None, ex.ActualStreamRevision);
}

func Test_appending_with_stream_exists_expected_version_and_stream_does_not_exist_returns_wrong_expected_version(t *testing.T) {
	//var stream = _fixture.GetStreamName();
	//
	//var writeResult = await _fixture.Client.AppendToStreamAsync(stream, StreamState.StreamExists,
	//	_fixture.CreateTestEvents(), options => {
	//	options.ThrowOnAppendFailure = false;
	//});
	//
	//var wrongExpectedVersionResult = Assert.IsType<WrongExpectedVersionResult>(writeResult);
	//
	//Assert.Equal(StreamRevision.None, wrongExpectedVersionResult.NextExpectedStreamRevision);
}

func Test_appending_with_stream_exists_expected_version_to_hard_deleted_stream_throws_stream_deleted(t *testing.T) {
	//var stream = _fixture.GetStreamName();
	//
	//await _fixture.Client.TombstoneAsync(stream, StreamState.NoStream);
	//
	//await Assert.ThrowsAsync<StreamDeletedException>(() => _fixture.Client.AppendToStreamAsync(
	//	stream,
	//	StreamState.StreamExists,
	//	_fixture.CreateTestEvents()));
}

func Test_appending_with_stream_exists_expected_version_to_soft_deleted_stream_throws_stream_deleted(t *testing.T) {
	//var stream = _fixture.GetStreamName();
	//
	//await _fixture.Client.SoftDeleteAsync(stream, StreamState.NoStream);
	//
	//await Assert.ThrowsAsync<StreamDeletedException>(() => _fixture.Client.AppendToStreamAsync(
	//	stream,
	//	StreamState.StreamExists,
	//	_fixture.CreateTestEvents()));
}

func Test_can_append_multiple_events_at_once(t *testing.T) {
	//var stream = _fixture.GetStreamName();
	//
	//var writeResult = await _fixture.Client.AppendToStreamAsync(
	//	stream, StreamState.NoStream, _fixture.CreateTestEvents(100));
	//
	//Assert.Equal(new StreamRevision(99), writeResult.NextExpectedStreamRevision);
}

func Test_returns_failure_status_when_conditionally_appending_with_version_mismatch(t *testing.T) {
	//var stream = _fixture.GetStreamName();
	//
	//var result = await _fixture.Client.ConditionalAppendToStreamAsync(stream, new StreamRevision(7),
	//	_fixture.CreateTestEvents());
	//
	//Assert.Equal(ConditionalWriteResult.FromWrongExpectedVersion(
	//	new WrongExpectedVersionException(stream, new StreamRevision(7), StreamRevision.None)),
	//result);
}

func Test_returns_success_status_when_conditionally_appending_with_matching_version(t *testing.T) {
	//var stream = _fixture.GetStreamName();
	//
	//var result = await _fixture.Client.ConditionalAppendToStreamAsync(stream, StreamState.Any,
	//	_fixture.CreateTestEvents());
	//
	//Assert.Equal(ConditionalWriteResult.FromWriteResult(new SuccessResult(0, result.LogPosition)),
	//result);
}

func Test_returns_failure_status_when_conditionally_appending_to_a_deleted_stream(t *testing.T) {
	//var stream = _fixture.GetStreamName();
	//
	//await _fixture.Client.AppendToStreamAsync(stream, StreamState.NoStream, _fixture.CreateTestEvents());
	//
	//await _fixture.Client.TombstoneAsync(stream, StreamState.Any);
	//
	//var result = await _fixture.Client.ConditionalAppendToStreamAsync(stream, StreamState.Any,
	//	_fixture.CreateTestEvents());
	//
	//Assert.Equal(ConditionalWriteResult.StreamDeleted, result);
}

//func collectStreamEvents(stream event_streams.ReadClient) ([]*messages.ResolvedEvent, error) {
//	events := []*messages.ResolvedEvent{}
//
//	for {
//		event, err := stream.Recv()
//		if err != nil {
//			if err == io.EOF {
//				break
//			}
//
//			return nil, err
//		}
//
//		events = append(events, event)
//	}
//
//	return events, nil
//}

//func TestAppendToStreamSingleEventNoStream(t *testing.T) {
//	container := GetEmptyDatabase()
//	defer container.Close()
//
//	client := CreateTestClient(container, t)
//	defer client.Close()
//	testEvent := event_streams.ProposedEvent{
//		EventID:      uuid.FromStringOrNil("38fffbc2-339e-11ea-8c7b-784f43837872"),
//		EventType:    "TestEvent",
//		ContentType:  "application/octet-stream",
//		UserMetadata: []byte{0xd, 0xe, 0xa, 0xd},
//		Data:         []byte{0xb, 0xe, 0xe, 0xf},
//	}
//	proposedEvents := []event_streams.ProposedEvent{
//		testEvent,
//	}
//
//	streamID, _ := uuid.NewV4()
//	context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
//	defer cancel()
//	_, err := client.AppendToStream(context, streamID.String(),
//		event_streams.AppendRequestExpectedStreamRevisionNoStream{}, proposedEvents)
//	if err != nil {
//		t.Fatalf("Unexpected failure %+v", err)
//	}
//
//	stream, err := client.ReadStreamEvents(context,
//		event_streams.ReadRequestDirectionForward,
//		streamID.String(),
//		event_streams.ReadRequestOptionsStreamRevisionStart{},
//		1,
//		false)
//	if err != nil {
//		t.Fatalf("Unexpected failure %+v", err)
//	}
//
//	defer stream.Close()
//
//	events, err := collectStreamEvents(stream)
//	if err != nil {
//		t.Fatalf("Unexpected failure %+v", err)
//	}
//
//	assert.Equal(t, int32(1), int32(len(events)), "Expected the correct number of messages to be returned")
//	assert.Equal(t, testEvent.EventID, events[0].GetOriginalEvent().EventID)
//	assert.Equal(t, testEvent.EventType, events[0].GetOriginalEvent().EventType)
//	assert.Equal(t, streamID.String(), events[0].GetOriginalEvent().StreamID)
//	assert.Equal(t, testEvent.Data, events[0].GetOriginalEvent().Data)
//	assert.Equal(t, testEvent.UserMetadata, events[0].GetOriginalEvent().UserMetadata)
//}

//func TestAppendWithInvalidStreamRevision(t *testing.T) {
//	container := GetEmptyDatabase()
//	defer container.Close()
//
//	client := CreateTestClient(container, t)
//	defer client.Close()
//	events := []messages.ProposedEvent{
//		createTestEvent(),
//	}
//
//	streamID, _ := uuid.NewV4()
//	context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
//	defer cancel()
//	_, err := client.AppendToStream_OLD(context, streamID.String(), stream_revision.StreamRevisionStreamExists, events)
//
//	if !errors.Is(err, client_errors.ErrWrongExpectedStreamRevision) {
//		t.Fatalf("Expected WrongExpectedVersion, got %+v", err)
//	}
//}

//func TestAppendToSystemStreamWithIncorrectCredentials(t *testing.T) {
//	container := GetEmptyDatabase()
//	defer container.Close()
//
//	conn := fmt.Sprintf("esdb://bad_user:bad_password@%s?tlsverifycert=false", container.Endpoint)
//	config, err := client.ParseConnectionString(conn)
//	if err != nil {
//		t.Fatalf("Unexpected configuration error: %s", err.Error())
//	}
//
//	client, err := client.NewClient(config)
//	if err != nil {
//		t.Fatalf("Unexpected failure setting up test connection: %s", err.Error())
//	}
//
//	defer client.Close()
//	events := []messages.ProposedEvent{
//		createTestEvent(),
//	}
//
//	streamID, _ := uuid.NewV4()
//	context, cancel := context.WithTimeout(context.Background(), time.Duration(5)*time.Second)
//	defer cancel()
//	_, err = client.AppendToStream_OLD(context, streamID.String(), stream_revision.StreamRevisionAny, events)
//
//	if !errors.Is(err, client_errors.ErrUnauthenticated) {
//		t.Fatalf("Expected Unauthenticated, got %+v", err)
//	}
//}
