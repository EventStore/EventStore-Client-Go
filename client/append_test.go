package client_test

import (
	"context"
	"testing"

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
	require.EqualError(t, err, event_streams.StreamNotFoundErr)
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
	require.EqualError(t, err, event_streams.StreamNotFoundErr)
}

func Test_create_stream_expected_version_on_first_write_if_does_not_exist_expectedStreamAny(t *testing.T) {
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

func Test_create_stream_expected_version_on_first_write_if_does_not_exist_expectedStreamNoStrea(t *testing.T) {
	// expectedStreamState:= event_streams.AppendRequestExpectedStreamRevisionNoStream{}

	//var stream = $"{_fixture.GetStreamName()}_{expectedStreamState}";
	//
	//var writeResult = await _fixture.Client.AppendToStreamAsync(
	//	stream,
	//	expectedStreamState,
	//	_fixture.CreateTestEvents(1));
	//
	//Assert.Equal(new StreamRevision(0), writeResult.NextExpectedStreamRevision);
	//
	//var count = await _fixture.Client.ReadStreamAsync(Direction.Forwards, stream, StreamPosition.Start, 2)
	//.CountAsync();
	//Assert.Equal(1, count);
}

func Test_multiple_idempotent_writes_expectedStreamAny(t *testing.T) {
	// expectedStreamState:= event_streams.AppendRequestExpectedStreamRevisionAny{}

	//var stream = _fixture.GetStreamName();
	//var events = _fixture.CreateTestEvents(4).ToArray();
	//
	//var writeResult = await _fixture.Client.AppendToStreamAsync(stream, StreamState.Any, events);
	//Assert.Equal(new StreamRevision(3), writeResult.NextExpectedStreamRevision);
	//
	//writeResult = await _fixture.Client.AppendToStreamAsync(stream, StreamState.Any, events);
	//Assert.Equal(new StreamRevision(3), writeResult.NextExpectedStreamRevision);
}

func Test_multiple_idempotent_writes_with_same_id_bug_case_expectedStreamAny(t *testing.T) {
	//var stream = _fixture.GetStreamName();
	//
	//var evnt = _fixture.CreateTestEvents().First();
	//var events = new[] {evnt, evnt, evnt, evnt, evnt, evnt};
	//
	//var writeResult = await _fixture.Client.AppendToStreamAsync(stream, StreamState.Any, events);
	//
	//Assert.Equal(new StreamRevision(5), writeResult.NextExpectedStreamRevision);
}

func Test_in_case_where_multiple_writes_of_multiple_events_with_the_same_ids_using_expected_version_any_then_next_expected_version_is_unreliable(t *testing.T) {
	//var stream = _fixture.GetStreamName();
	//
	//var evnt = _fixture.CreateTestEvents().First();
	//var events = new[] {evnt, evnt, evnt, evnt, evnt, evnt};
	//
	//var writeResult = await _fixture.Client.AppendToStreamAsync(stream, StreamState.Any, events);
	//
	//Assert.Equal(new StreamRevision(5), writeResult.NextExpectedStreamRevision);
	//
	//writeResult = await _fixture.Client.AppendToStreamAsync(stream, StreamState.Any, events);
	//
	//Assert.Equal(new StreamRevision(0), writeResult.NextExpectedStreamRevision);
}

func Test_in_case_where_multiple_writes_of_multiple_events_with_the_same_ids_using_expected_version_nostream_then_next_expected_version_is_correct(t *testing.T) {
	//var stream = _fixture.GetStreamName();
	//
	//var evnt = _fixture.CreateTestEvents().First();
	//var events = new[] {evnt, evnt, evnt, evnt, evnt, evnt};
	//var streamRevision = StreamRevision.FromInt64(events.Length - 1);
	//
	//var writeResult = await _fixture.Client.AppendToStreamAsync(stream, StreamState.NoStream, events);
	//
	//Assert.Equal(streamRevision, writeResult.NextExpectedStreamRevision);
	//
	//writeResult = await _fixture.Client.AppendToStreamAsync(stream, StreamState.NoStream, events);
	//
	//Assert.Equal(streamRevision, writeResult.NextExpectedStreamRevision);
}

func Test_writing_with_correct_expected_version_to_deleted_stream_throws_stream_deleted(t *testing.T) {
	//var stream = _fixture.GetStreamName();
	//
	//await _fixture.Client.TombstoneAsync(stream, StreamState.NoStream);
	//
	//await Assert.ThrowsAsync<StreamDeletedException>(() => _fixture.Client.AppendToStreamAsync(
	//	stream,
	//	StreamState.NoStream,
	//	_fixture.CreateTestEvents(1)));
}

func Test_returns_log_position_when_writing(t *testing.T) {
	//var stream = _fixture.GetStreamName();
	//
	//var result = await _fixture.Client.AppendToStreamAsync(
	//	stream,
	//	StreamState.NoStream,
	//	_fixture.CreateTestEvents(1));
	//Assert.True(0 < result.LogPosition.PreparePosition);
	//Assert.True(0 < result.LogPosition.CommitPosition);
}

func Test_writing_with_any_expected_version_to_deleted_stream_throws_stream_deleted(t *testing.T) {
	//var stream = _fixture.GetStreamName();
	//await _fixture.Client.TombstoneAsync(stream, StreamState.NoStream);
	//
	//await Assert.ThrowsAsync<StreamDeletedException>(
	//	() => _fixture.Client.AppendToStreamAsync(stream, StreamState.Any, _fixture.CreateTestEvents(1)));
}

func Test_writing_with_invalid_expected_version_to_deleted_stream_throws_stream_deleted(t *testing.T) {
	//var stream = _fixture.GetStreamName();
	//
	//await _fixture.Client.TombstoneAsync(stream, StreamState.NoStream);
	//
	//await Assert.ThrowsAsync<StreamDeletedException>(
	//	() => _fixture.Client.AppendToStreamAsync(stream, new StreamRevision(5), _fixture.CreateTestEvents()));
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
