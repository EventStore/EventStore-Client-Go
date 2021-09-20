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

func Test_AppendZeroEvents_ToNonExistingStream(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()
	client := CreateTestClient(container, t)
	defer func() {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}()

	t.Run("With Expected Revision NoStream", func(t *testing.T) {
		streamName := "test_no_stream"

		iterations := 2
		for ; iterations > 0; iterations-- {
			writeResult, err := client.AppendToStream(context.Background(),
				streamName,
				event_streams.AppendRequestExpectedStreamRevisionNoStream{},
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
	})

	t.Run("With Expected Revision Any", func(t *testing.T) {
		streamName := "test_any"

		iterations := 2
		for ; iterations > 0; iterations-- {
			writeResult, err := client.AppendToStream(context.Background(),
				streamName,
				event_streams.AppendRequestExpectedStreamRevisionAny{},
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
	})
}

func Test_AppendToNonExistingStream_WithExpectedRevision(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()
	client := CreateTestClient(container, t)
	defer func() {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}()

	t.Run("With Expected Revision Any", func(t *testing.T) {
		streamName := "stream_any"

		testEvent := createTestEvent()

		writeResult, err := client.AppendToStream(context.Background(),
			streamName,
			event_streams.AppendRequestExpectedStreamRevisionAny{},
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
	})

	t.Run("With Expected Revision NoStream, Append One By One", func(t *testing.T) {
		streamName := "stream_one_by_one"

		testEvent := createTestEvent()

		writeResult, err := client.AppendToStream(context.Background(),
			streamName,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
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
	})

	t.Run("With Expected Revision NoStream, Append Multiple At Once", func(t *testing.T) {
		streamName := "stream_multiple_at_once"

		testEvents := testCreateEvents(100)

		writeResult, err := client.AppendToStream(context.Background(),
			streamName,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			testEvents)
		require.NoError(t, err)
		currentRevision, isCurrentRevision := writeResult.GetCurrentRevision()
		require.True(t, isCurrentRevision)
		require.EqualValues(t, 99, currentRevision)
	})
}

func Test_AppendToExpectedStreamAny_Idempotency(t *testing.T) {
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

func Test_AppendMultipleEventsWithSameIds_WithExpectedRevisionAny_BugCase(t *testing.T) {
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

func Test_AppendMultipleEventsWithSameIds_WithExpectedRevisionAny_NextExpectedVersionIsUnreliable(t *testing.T) {
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

func Test_AppendMultipleEventsWithSameIds_WithExpectedRevisionNoStream_NextExpectedVersionIsUnreliable(t *testing.T) {
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

func Test_AppendToDeletedStream_StreamDeletedErr(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()
	client := CreateTestClient(container, t)
	defer func() {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}()

	t.Run("Stream Does Not Exist, Revision Any", func(t *testing.T) {
		streamName := "stream_does_not_exist_any"

		_, err := client.TombstoneStreamNoStreamRevision(context.Background(), streamName)
		require.NoError(t, err)

		event := createTestEvent()
		expectedStreamRevision := event_streams.AppendRequestExpectedStreamRevisionAny{}

		_, err = client.AppendToStream(context.Background(),
			streamName,
			expectedStreamRevision,
			[]event_streams.ProposedEvent{event})
		require.Equal(t, errors.StreamDeletedErr, err.Code())
	})

	t.Run("Stream Does Not Exist, Revision NoStream", func(t *testing.T) {
		streamName := "stream_does_not_exist_no_stream"

		_, err := client.TombstoneStreamNoStreamRevision(context.Background(), streamName)
		require.NoError(t, err)

		event := createTestEvent()
		expectedStreamRevision := event_streams.AppendRequestExpectedStreamRevisionNoStream{}

		_, err = client.AppendToStream(context.Background(),
			streamName,
			expectedStreamRevision,
			[]event_streams.ProposedEvent{event})
		require.Equal(t, errors.StreamDeletedErr, err.Code())
	})

	t.Run("Stream Does Not Exist, Invalid Finite Revision", func(t *testing.T) {
		streamName := "stream_does_not_exist_invalid_finite"

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
	})

	t.Run("Existing Stream Is Tombstoned As Any, Append With Revision Any", func(t *testing.T) {
		expectedStreamRevision := event_streams.AppendRequestExpectedStreamRevisionNoStream{}
		streamName := "existing_stream_tombstoned_append_with_revision_any"

		testEvent := createTestEvent()

		_, err := client.AppendToStream(context.Background(),
			streamName,
			expectedStreamRevision,
			[]event_streams.ProposedEvent{testEvent})
		require.NoError(t, err)

		_, err = client.TombstoneStreamAnyRevision(context.Background(), streamName)
		require.NoError(t, err)

		testEvent2 := createTestEvent()
		_, err = client.AppendToStream(context.Background(),
			streamName,
			expectedStreamRevision,
			[]event_streams.ProposedEvent{testEvent2})
		require.Equal(t, errors.StreamDeletedErr, err.Code())
	})
}

func Test_AppendToExistingStream(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()
	client := CreateTestClient(container, t)
	defer func() {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}()

	t.Run("First Append With NoStream, Append With Expected Revision Finite", func(t *testing.T) {
		expectedStreamRevision := event_streams.AppendRequestExpectedStreamRevisionNoStream{}
		streamName := "stream_first_append_no_stream_and_expected_revision_finite"

		testEvent := createTestEvent()

		writeResult, err := client.AppendToStream(context.Background(),
			streamName,
			expectedStreamRevision,
			[]event_streams.ProposedEvent{testEvent})
		require.NoError(t, err)
		success, _ := writeResult.GetSuccess()

		testEvent2 := createTestEvent()
		writeResult, err = client.AppendToStream(context.Background(),
			streamName,
			event_streams.AppendRequestExpectedStreamRevision{
				Revision: success.GetCurrentRevision(),
			},
			[]event_streams.ProposedEvent{testEvent2})
		require.NoError(t, err)

		success, _ = writeResult.GetSuccess()
		require.EqualValues(t, 1, success.GetCurrentRevision())
	})

	t.Run("First Append With NoStream, Append With Expected Revision Any", func(t *testing.T) {
		expectedStreamRevision := event_streams.AppendRequestExpectedStreamRevisionNoStream{}
		streamName := "stream_first_append_no_stream_and_append_with_any"

		testEvent := createTestEvent()

		writeResult, err := client.AppendToStream(context.Background(),
			streamName,
			expectedStreamRevision,
			[]event_streams.ProposedEvent{testEvent})
		require.NoError(t, err)
		success, _ := writeResult.GetSuccess()
		require.EqualValues(t, 0, success.GetCurrentRevision())

		testEvent2 := createTestEvent()
		writeResult, err = client.AppendToStream(context.Background(),
			streamName,
			event_streams.AppendRequestExpectedStreamRevisionAny{},
			[]event_streams.ProposedEvent{testEvent2})
		require.NoError(t, err)

		success, _ = writeResult.GetSuccess()
		require.EqualValues(t, 1, success.GetCurrentRevision())
	})

	t.Run("First Append With NoStream, Append With Expected Revision StreamExists", func(t *testing.T) {
		expectedStreamRevision := event_streams.AppendRequestExpectedStreamRevisionNoStream{}
		streamName := "stream_first_append_no_stream_and_append_with_stream_exists"

		testEvent := createTestEvent()

		writeResult, err := client.AppendToStream(context.Background(),
			streamName,
			expectedStreamRevision,
			[]event_streams.ProposedEvent{testEvent})
		require.NoError(t, err)
		success, _ := writeResult.GetSuccess()
		require.EqualValues(t, 0, success.GetCurrentRevision())

		testEvent2 := createTestEvent()
		writeResult, err = client.AppendToStream(context.Background(),
			streamName,
			event_streams.AppendRequestExpectedStreamRevisionStreamExists{},
			[]event_streams.ProposedEvent{testEvent2})
		require.NoError(t, err)

		success, _ = writeResult.GetSuccess()
		require.EqualValues(t, 1, success.GetCurrentRevision())
	})

	t.Run("First Append With Any, Append With Expected Revision StreamExists", func(t *testing.T) {
		expectedStreamRevision := event_streams.AppendRequestExpectedStreamRevisionAny{}
		streamName := "stream_first_append_any_and_append_with_stream_exists"

		testEvent := createTestEvent()

		writeResult, err := client.AppendToStream(context.Background(),
			streamName,
			expectedStreamRevision,
			[]event_streams.ProposedEvent{testEvent})
		require.NoError(t, err)
		success, _ := writeResult.GetSuccess()
		require.EqualValues(t, 0, success.GetCurrentRevision())

		testEvent2 := createTestEvent()
		writeResult, err = client.AppendToStream(context.Background(),
			streamName,
			event_streams.AppendRequestExpectedStreamRevisionStreamExists{},
			[]event_streams.ProposedEvent{testEvent2})
		require.NoError(t, err)

		success, _ = writeResult.GetSuccess()
		require.EqualValues(t, 1, success.GetCurrentRevision())
	})

	t.Run("Stream Does Not Exist, Append With Expected Revision StreamExists", func(t *testing.T) {
		streamName := "stream_does_not_exist_append_with_stream_exists"

		testEvent := createTestEvent()

		writeResult, err := client.AppendToStream(context.Background(),
			streamName,
			event_streams.AppendRequestExpectedStreamRevisionStreamExists{},
			[]event_streams.ProposedEvent{testEvent})
		require.NoError(t, err)

		require.True(t, writeResult.IsCurrentRevisionNoStream())
	})

	t.Run("Tombstone Stream And Append With Expected Revision StreamExists", func(t *testing.T) {
		streamName := "stream_hard_deleted"

		_, err := client.TombstoneStreamNoStreamRevision(context.Background(), streamName)
		require.NoError(t, err)

		event := createTestEvent()

		_, err = client.AppendToStream(context.Background(),
			streamName,
			event_streams.AppendRequestExpectedStreamRevisionStreamExists{},
			[]event_streams.ProposedEvent{event})
		require.Equal(t, errors.StreamDeletedErr, err.Code())
	})

	t.Run("Stream Deleted (soft delete) Before Append With Expected Revision StreamExists", func(t *testing.T) {
		streamName := "stream_soft_deleted"

		_, err := client.DeleteStreamRevisionNoStream(context.Background(), streamName)
		require.NoError(t, err)

		event := createTestEvent()

		_, err = client.AppendToStream(context.Background(),
			streamName,
			event_streams.AppendRequestExpectedStreamRevisionStreamExists{},
			[]event_streams.ProposedEvent{event})
		require.Equal(t, errors.StreamDeletedErr, err.Code())
	})

	t.Run("With Wrong Expected Revision When Stream Already Exists", func(t *testing.T) {
		expectedStreamRevision := event_streams.AppendRequestExpectedStreamRevisionNoStream{}
		streamName := "wrong_expected_revision_stream_already_exists"

		testEvent := createTestEvent()

		_, err := client.AppendToStream(context.Background(),
			streamName,
			expectedStreamRevision,
			[]event_streams.ProposedEvent{testEvent})
		require.NoError(t, err)

		testEvent2 := createTestEvent()
		writeResult, err := client.AppendToStream(context.Background(),
			streamName,
			event_streams.AppendRequestExpectedStreamRevision{
				Revision: 999,
			},
			[]event_streams.ProposedEvent{testEvent2})
		require.NoError(t, err)

		currentRevision, isFiniteCurrentRevision := writeResult.GetWrongCurrentRevision()
		require.True(t, isFiniteCurrentRevision)
		require.EqualValues(t, 0, currentRevision)

		expectedRevision, isFiniteExpectedRevision := writeResult.GetWrongExpectedRevision()
		require.True(t, isFiniteExpectedRevision)
		require.EqualValues(t, 999, expectedRevision)
	})

	t.Run("With Wrong Expected Revision When Stream Does Not Exist", func(t *testing.T) {
		streamName := "wrong_expected_revision_stream_does_not_exist"

		testEvent := createTestEvent()

		writeResult, err := client.AppendToStream(context.Background(),
			streamName,
			event_streams.AppendRequestExpectedStreamRevision{
				Revision: 5,
			},
			[]event_streams.ProposedEvent{testEvent})
		require.NoError(t, err)

		require.True(t, writeResult.IsCurrentRevisionNoStream())

		expectedRevision, isFiniteExpectedRevision := writeResult.GetWrongExpectedRevision()
		require.True(t, isFiniteExpectedRevision)
		require.EqualValues(t, 5, expectedRevision)
	})
}

func Test_AppendToExistingStream_WithWrongExpectedRevision_Finite_WrongExpectedVersionResult(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()
	client := CreateTestClient(container, t)
	defer func() {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}()
}

func Test_AppendToNonExistingStream_WithWrongExpectedRevision_Finite_WrongExpectedVersionResult(t *testing.T) {
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

	testEvent := createTestEvent()

	writeResult, err := client.AppendToStream(context.Background(),
		streamName,
		event_streams.AppendRequestExpectedStreamRevision{
			Revision: 1,
		},
		[]event_streams.ProposedEvent{testEvent})
	require.NoError(t, err)
	wrongExpectedVersion, isWrongExpectedVersion := writeResult.GetWrongExpectedVersion()
	require.True(t, isWrongExpectedVersion)
	expectedRevision, isFiniteExpectedRevision := wrongExpectedVersion.GetExpectedRevision()
	require.True(t, isFiniteExpectedRevision)
	require.EqualValues(t, 1, expectedRevision)
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
