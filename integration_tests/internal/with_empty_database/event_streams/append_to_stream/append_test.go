package append_test

import (
	"context"
	"testing"

	"github.com/pivonroll/EventStore-Client-Go/errors"
	"github.com/pivonroll/EventStore-Client-Go/event_streams"
	"github.com/pivonroll/EventStore-Client-Go/test_utils"
	"github.com/stretchr/testify/require"
)

func Test_AppendZeroEvents_ToNonExistingStream(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	t.Run("With Expected Revision NoStream", func(t *testing.T) {
		streamName := "test_no_stream"

		iterations := 2
		for ; iterations > 0; iterations-- {
			writeResult, err := client.AppendToStream(context.Background(),
				streamName,
				event_streams.WriteStreamRevisionNoStream{},
				[]event_streams.ProposedEvent{})
			require.NoError(t, err)
			require.True(t, writeResult.IsCurrentRevisionNoStream())
		}

		_, err := client.ReadStreamEvents(context.Background(),
			streamName,
			event_streams.ReadDirectionForward,
			event_streams.ReadStreamRevisionStart{},
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
				event_streams.WriteStreamRevisionAny{},
				[]event_streams.ProposedEvent{})
			require.NoError(t, err)
			require.True(t, writeResult.IsCurrentRevisionNoStream())
		}

		_, err := client.ReadStreamEvents(context.Background(),
			streamName,
			event_streams.ReadDirectionForward,
			event_streams.ReadStreamRevisionStart{},
			2,
			false)
		require.Equal(t, err.Code(), errors.StreamNotFoundErr)
	})
}

func Test_AppendToNonExistingStream_WithExpectedRevision(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	t.Run("With Expected Revision Any", func(t *testing.T) {
		streamName := "stream_any"

		testEvent := testCreateEvent()

		writeResult, err := client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionAny{},
			[]event_streams.ProposedEvent{testEvent})
		require.NoError(t, err)
		require.EqualValues(t, 0, writeResult.GetCurrentRevision())

		events, err := client.ReadStreamEvents(context.Background(),
			streamName,
			event_streams.ReadDirectionForward,
			event_streams.ReadStreamRevisionStart{},
			2,
			false)
		require.NoError(t, err)
		require.Len(t, events, 1)
	})

	t.Run("With Expected Revision NoStream, Append One By One", func(t *testing.T) {
		streamName := "stream_one_by_one"

		testEvent := testCreateEvent()

		writeResult, err := client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionNoStream{},
			[]event_streams.ProposedEvent{testEvent})
		require.NoError(t, err)
		require.EqualValues(t, 0, writeResult.GetCurrentRevision())

		events, err := client.ReadStreamEvents(context.Background(),
			streamName,
			event_streams.ReadDirectionForward,
			event_streams.ReadStreamRevisionStart{},
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
			event_streams.WriteStreamRevisionNoStream{},
			testEvents)
		require.NoError(t, err)
		require.EqualValues(t, 99, writeResult.GetCurrentRevision())
	})
}

func Test_AppendToExpectedStreamAny_Idempotency(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	expectedStreamRevision := event_streams.WriteStreamRevisionAny{}
	streamName := "stream_any"
	events := testCreateEvents(4)

	writeResult, err := client.AppendToStream(context.Background(),
		streamName,
		expectedStreamRevision,
		events)
	require.NoError(t, err)
	require.EqualValues(t, 3, writeResult.GetCurrentRevision())

	writeResult, err = client.AppendToStream(context.Background(),
		streamName,
		expectedStreamRevision,
		events)
	require.NoError(t, err)
	require.EqualValues(t, 3, writeResult.GetCurrentRevision())
}

func Test_AppendMultipleEventsWithSameIds_WithExpectedRevisionAny_BugCase(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	expectedStreamRevision := event_streams.WriteStreamRevisionAny{}
	streamName := "stream_any"
	event := testCreateEvent()
	events := []event_streams.ProposedEvent{event, event, event, event, event, event}

	writeResult, err := client.AppendToStream(context.Background(),
		streamName,
		expectedStreamRevision,
		events)
	require.NoError(t, err)
	require.EqualValues(t, 5, writeResult.GetCurrentRevision())
}

func Test_AppendMultipleEventsWithSameIds_WithExpectedRevisionAny_NextExpectedVersionIsUnreliable(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	expectedStreamRevision := event_streams.WriteStreamRevisionAny{}
	streamName := "stream_any"
	event := testCreateEvent()
	events := []event_streams.ProposedEvent{event, event, event, event, event, event}

	writeResult, err := client.AppendToStream(context.Background(),
		streamName,
		expectedStreamRevision,
		events)
	require.NoError(t, err)
	require.EqualValues(t, 5, writeResult.GetCurrentRevision())

	writeResult, err = client.AppendToStream(context.Background(),
		streamName,
		expectedStreamRevision,
		events)
	require.NoError(t, err)
	require.EqualValues(t, 0, writeResult.GetCurrentRevision())
}

func Test_AppendMultipleEventsWithSameIds_WithExpectedRevisionNoStream_NextExpectedVersionIsUnreliable(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	expectedStreamRevision := event_streams.WriteStreamRevisionNoStream{}
	streamName := "stream_no_stream"
	event := testCreateEvent()
	events := []event_streams.ProposedEvent{event, event, event, event, event, event}

	writeResult, err := client.AppendToStream(context.Background(),
		streamName,
		expectedStreamRevision,
		events)
	require.NoError(t, err)
	require.EqualValues(t, 5, writeResult.GetCurrentRevision())

	writeResult, err = client.AppendToStream(context.Background(),
		streamName,
		expectedStreamRevision,
		events)
	require.NoError(t, err)
	require.EqualValues(t, 5, writeResult.GetCurrentRevision())
}

func Test_ReturnsPositionWhenWriting(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	streamName := "stream_no_stream"
	event := testCreateEvent()
	expectedStreamRevision := event_streams.WriteStreamRevisionNoStream{}
	writeResult, _ := client.AppendToStream(context.Background(),
		streamName,
		expectedStreamRevision,
		[]event_streams.ProposedEvent{event})

	position, _ := writeResult.GetPosition()
	require.Greater(t, position.PreparePosition, uint64(0))
	require.Greater(t, position.CommitPosition, uint64(0))
}

func Test_AppendToDeletedStream_StreamDeletedErr(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	t.Run("Stream Does Not Exist, Revision Any", func(t *testing.T) {
		streamName := "stream_does_not_exist_any"

		_, err := client.TombstoneStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionNoStream{})
		require.NoError(t, err)

		event := testCreateEvent()
		expectedStreamRevision := event_streams.WriteStreamRevisionAny{}

		_, err = client.AppendToStream(context.Background(),
			streamName,
			expectedStreamRevision,
			[]event_streams.ProposedEvent{event})
		require.Equal(t, errors.StreamDeletedErr, err.Code())
	})

	t.Run("Stream Does Not Exist, Revision NoStream", func(t *testing.T) {
		streamName := "stream_does_not_exist_no_stream"

		_, err := client.TombstoneStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionNoStream{})
		require.NoError(t, err)

		event := testCreateEvent()
		expectedStreamRevision := event_streams.WriteStreamRevisionNoStream{}

		_, err = client.AppendToStream(context.Background(),
			streamName,
			expectedStreamRevision,
			[]event_streams.ProposedEvent{event})
		require.Equal(t, errors.StreamDeletedErr, err.Code())
	})

	t.Run("Stream Does Not Exist, Invalid Finite Revision", func(t *testing.T) {
		streamName := "stream_does_not_exist_invalid_finite"

		_, err := client.TombstoneStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionNoStream{})
		require.NoError(t, err)

		event := testCreateEvent()
		expectedStreamRevision := event_streams.WriteStreamRevision{
			Revision: 5,
		}

		_, err = client.AppendToStream(context.Background(),
			streamName,
			expectedStreamRevision,
			[]event_streams.ProposedEvent{event})
		require.Equal(t, errors.StreamDeletedErr, err.Code())
	})

	t.Run("Existing Stream Is Tombstoned As Any, Append With Revision Any", func(t *testing.T) {
		expectedStreamRevision := event_streams.WriteStreamRevisionNoStream{}
		streamName := "existing_stream_tombstoned_append_with_revision_any"

		testEvent := testCreateEvent()

		_, err := client.AppendToStream(context.Background(),
			streamName,
			expectedStreamRevision,
			[]event_streams.ProposedEvent{testEvent})
		require.NoError(t, err)

		_, err = client.TombstoneStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionAny{})
		require.NoError(t, err)

		testEvent2 := testCreateEvent()
		_, err = client.AppendToStream(context.Background(),
			streamName,
			expectedStreamRevision,
			[]event_streams.ProposedEvent{testEvent2})
		require.Equal(t, errors.StreamDeletedErr, err.Code())
	})
}

func Test_AppendToExistingStream(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	t.Run("First Append With NoStream, Append With Finite Expected Revision ", func(t *testing.T) {
		expectedStreamRevision := event_streams.WriteStreamRevisionNoStream{}
		streamName := "stream_first_append_no_stream_and_expected_revision_finite"

		testEvent := testCreateEvent()

		writeResult, err := client.AppendToStream(context.Background(),
			streamName,
			expectedStreamRevision,
			[]event_streams.ProposedEvent{testEvent})
		require.NoError(t, err)

		testEvent2 := testCreateEvent()
		writeResult, err = client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevision{
				Revision: writeResult.GetCurrentRevision(),
			},
			[]event_streams.ProposedEvent{testEvent2})
		require.NoError(t, err)

		require.EqualValues(t, 1, writeResult.GetCurrentRevision())
	})

	t.Run("First Append With NoStream, Append With Expected Revision Any", func(t *testing.T) {
		expectedStreamRevision := event_streams.WriteStreamRevisionNoStream{}
		streamName := "stream_first_append_no_stream_and_append_with_any"

		testEvent := testCreateEvent()

		writeResult, err := client.AppendToStream(context.Background(),
			streamName,
			expectedStreamRevision,
			[]event_streams.ProposedEvent{testEvent})
		require.NoError(t, err)
		require.EqualValues(t, 0, writeResult.GetCurrentRevision())

		testEvent2 := testCreateEvent()
		writeResult, err = client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionAny{},
			[]event_streams.ProposedEvent{testEvent2})
		require.NoError(t, err)

		require.EqualValues(t, 1, writeResult.GetCurrentRevision())
	})

	t.Run("First Append With NoStream, Append With Expected Revision StreamExists", func(t *testing.T) {
		expectedStreamRevision := event_streams.WriteStreamRevisionNoStream{}
		streamName := "stream_first_append_no_stream_and_append_with_stream_exists"

		testEvent := testCreateEvent()

		writeResult, err := client.AppendToStream(context.Background(),
			streamName,
			expectedStreamRevision,
			[]event_streams.ProposedEvent{testEvent})
		require.NoError(t, err)
		require.EqualValues(t, 0, writeResult.GetCurrentRevision())

		testEvent2 := testCreateEvent()
		writeResult, err = client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionStreamExists{},
			[]event_streams.ProposedEvent{testEvent2})
		require.NoError(t, err)

		require.EqualValues(t, 1, writeResult.GetCurrentRevision())
	})

	t.Run("First Append With Any, Append With Expected Revision StreamExists", func(t *testing.T) {
		expectedStreamRevision := event_streams.WriteStreamRevisionAny{}
		streamName := "stream_first_append_any_and_append_with_stream_exists"

		testEvent := testCreateEvent()

		writeResult, err := client.AppendToStream(context.Background(),
			streamName,
			expectedStreamRevision,
			[]event_streams.ProposedEvent{testEvent})
		require.NoError(t, err)
		require.EqualValues(t, 0, writeResult.GetCurrentRevision())

		testEvent2 := testCreateEvent()
		writeResult, err = client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionStreamExists{},
			[]event_streams.ProposedEvent{testEvent2})
		require.NoError(t, err)

		require.EqualValues(t, 1, writeResult.GetCurrentRevision())
	})

	t.Run("Stream Does Not Exist, Append With Expected Revision StreamExists", func(t *testing.T) {
		streamName := "stream_does_not_exist_append_with_stream_exists"

		testEvent := testCreateEvent()

		_, err := client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionStreamExists{},
			[]event_streams.ProposedEvent{testEvent})
		require.Equal(t, event_streams.WrongExpectedVersionErr, err.Code())
		wrongExpectedVersion := err.(event_streams.WrongExpectedVersion)
		require.True(t, wrongExpectedVersion.IsCurrentRevisionNoStream())
	})

	t.Run("Tombstone Stream And Append With Expected Revision StreamExists", func(t *testing.T) {
		streamName := "stream_hard_deleted"

		_, err := client.TombstoneStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionNoStream{})
		require.NoError(t, err)

		event := testCreateEvent()

		_, err = client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionStreamExists{},
			[]event_streams.ProposedEvent{event})
		require.Equal(t, errors.StreamDeletedErr, err.Code())
	})

	t.Run("Stream Deleted (soft delete) Before Append With Expected Revision StreamExists", func(t *testing.T) {
		streamName := "stream_soft_deleted"

		_, err := client.DeleteStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionNoStream{})
		require.NoError(t, err)

		event := testCreateEvent()

		_, err = client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionStreamExists{},
			[]event_streams.ProposedEvent{event})
		require.Equal(t, errors.StreamDeletedErr, err.Code())
	})

	t.Run("With Wrong Expected Revision When Stream Already Exists", func(t *testing.T) {
		expectedStreamRevision := event_streams.WriteStreamRevisionNoStream{}
		streamName := "wrong_expected_revision_stream_already_exists"

		testEvent := testCreateEvent()

		_, err := client.AppendToStream(context.Background(),
			streamName,
			expectedStreamRevision,
			[]event_streams.ProposedEvent{testEvent})
		require.NoError(t, err)

		testEvent2 := testCreateEvent()
		_, err = client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevision{
				Revision: 999,
			},
			[]event_streams.ProposedEvent{testEvent2})
		require.Equal(t, event_streams.WrongExpectedVersionErr, err.Code())
		wrongExpectedVersion := err.(event_streams.WrongExpectedVersion)

		currentRevision, isFiniteCurrentRevision := wrongExpectedVersion.GetCurrentRevision()
		require.True(t, isFiniteCurrentRevision)
		require.EqualValues(t, 0, currentRevision)

		require.True(t, wrongExpectedVersion.IsExpectedRevisionFinite())
		expectedRevision := wrongExpectedVersion.GetExpectedRevision()
		require.EqualValues(t, 999, expectedRevision)
	})

	t.Run("With Wrong Expected Revision When Stream Does Not Exist", func(t *testing.T) {
		streamName := "wrong_expected_revision_stream_does_not_exist"

		testEvent := testCreateEvent()

		_, err := client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevision{
				Revision: 5,
			},
			[]event_streams.ProposedEvent{testEvent})
		require.Equal(t, event_streams.WrongExpectedVersionErr, err.Code())
		wrongExpectedVersion := err.(event_streams.WrongExpectedVersion)

		require.True(t, wrongExpectedVersion.IsCurrentRevisionNoStream())

		require.True(t, wrongExpectedVersion.IsExpectedRevisionFinite())
		expectedRevision := wrongExpectedVersion.GetExpectedRevision()
		require.EqualValues(t, 5, expectedRevision)
	})
}

func Test_AppendToNonExistingStream_WithWrongExpectedRevision_Finite_WrongExpectedVersionError(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	streamName := "AppendToNonExistingStream_WithWrongExpectedRevision_Finite_WrongExpectedVersionError"

	testEvent := testCreateEvent()

	_, err := client.AppendToStream(context.Background(),
		streamName,
		event_streams.WriteStreamRevision{
			Revision: 1,
		},
		[]event_streams.ProposedEvent{testEvent})
	require.Equal(t, event_streams.WrongExpectedVersionErr, err.Code())
	wrongExpectedVersion := err.(event_streams.WrongExpectedVersion)
	require.True(t, wrongExpectedVersion.IsExpectedRevisionFinite())
	expectedRevision := wrongExpectedVersion.GetExpectedRevision()
	require.EqualValues(t, 1, expectedRevision)
}

func Test_AppendToStream_MetadataStreamExists_WithStreamExists(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	streamName := "AppendToStream_MetadataStreamExists_WithStreamExists"
	maxCount := 10
	streamMetadata := event_streams.StreamMetadata{MaxCount: &maxCount}

	_, err := client.SetStreamMetadata(context.Background(),
		streamName,
		event_streams.WriteStreamRevisionAny{},
		streamMetadata)
	require.NoError(t, err)

	testEvent := testCreateEvent()

	_, err = client.AppendToStream(context.Background(),
		streamName,
		event_streams.WriteStreamRevisionStreamExists{},
		[]event_streams.ProposedEvent{testEvent})
	require.NoError(t, err)
}

func Test_AppendToStream_WithAppendLimit(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t,
		map[string]string{
			string(test_utils.EVENTSTORE_MAX_APPEND_SIZE_IN_BYTES): "1024",
		})
	defer closeFunc()

	t.Run("Less than limit", func(t *testing.T) {
		streamName := "AppendToStream_WithAppendLimit_stream_less_than_limit"
		events := testCreateEventsWithBytesCap(1024)
		_, err := client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionNoStream{},
			events)
		require.NoError(t, err)
	})

	t.Run("More than limit", func(t *testing.T) {
		streamName := "AppendToStream_WithAppendLimit_stream_more_than_limit"
		events := testCreateEventsWithBytesCap(2056)
		_, err := client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionNoStream{},
			events)
		require.Equal(t, errors.MaximumAppendSizeExceededErr, err.Code())
	})
}

func Test_AppendMultipleEvents(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	t.Run("AppendMultipleEvents_sequence_0em1_1e0_2e1_3e2_4e3_5e4_0em1_idempotent", func(t *testing.T) {
		streamName := "AppendMultipleEvents_sequence_0em1_1e0_2e1_3e2_4e3_5e4_0em1_idempotent"

		events := testCreateEvents(6)

		_, err := client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		_, err = client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionNoStream{},
			[]event_streams.ProposedEvent{events[0]})
		require.NoError(t, err)

		readEvents, err := client.ReadStreamEvents(context.Background(),
			streamName,
			event_streams.ReadDirectionForward,
			event_streams.ReadStreamRevisionStart{},
			uint64(len(events)+1),
			false)
		require.NoError(t, err)
		require.Len(t, readEvents, len(events))
	})

	t.Run("AppendMultipleEvents_sequence_0em1_1e0_2e1_3e2_4e3_4e4_0any_idempotent", func(t *testing.T) {
		streamName := "AppendMultipleEvents_sequence_0em1_1e0_2e1_3e2_4e3_4e4_0any_idempotent"

		events := testCreateEvents(6)

		_, err := client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		_, err = client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionAny{},
			[]event_streams.ProposedEvent{events[0]})
		require.NoError(t, err)

		readEvents, err := client.ReadStreamEvents(context.Background(),
			streamName,
			event_streams.ReadDirectionForward,
			event_streams.ReadStreamRevisionStart{},
			uint64(len(events)+1),
			false)
		require.NoError(t, err)
		require.Len(t, readEvents, len(events))
	})

	t.Run("AppendMultipleEvents_sequence_0em1_1e0_2e1_3e2_4e3_5e4_0e5_non_idempotent", func(t *testing.T) {
		streamName := "AppendMultipleEvents_sequence_0em1_1e0_2e1_3e2_4e3_5e4_0e5_non_idempotent"

		events := testCreateEvents(6)

		_, err := client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		_, err = client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevision{
				Revision: 5,
			},
			[]event_streams.ProposedEvent{events[0]})
		require.NoError(t, err)

		readEvents, err := client.ReadStreamEvents(context.Background(),
			streamName,
			event_streams.ReadDirectionForward,
			event_streams.ReadStreamRevisionStart{},
			uint64(len(events)+2),
			false)
		require.NoError(t, err)
		require.Len(t, readEvents, len(events)+1)
	})

	t.Run("AppendMultipleEvents_sequence_0em1_1e0_2e1_3e2_4e3_5e4_0e6_returns_wev", func(t *testing.T) {
		streamName := "AppendMultipleEvents_sequence_0em1_1e0_2e1_3e2_4e3_5e4_0e6_returns_wev"

		events := testCreateEvents(6)

		_, err := client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		_, err = client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevision{
				Revision: 6,
			},
			[]event_streams.ProposedEvent{events[0]})
		require.Equal(t, event_streams.WrongExpectedVersionErr, err.Code())
	})

	t.Run("AppendMultipleEvents_sequence_0em1_1e0_2e1_3e2_4e3_5e4_0e4_returns_wev", func(t *testing.T) {
		streamName := "AppendMultipleEvents_sequence_0em1_1e0_2e1_3e2_4e3_5e4_0e4_returns_wev"

		events := testCreateEvents(6)

		_, err := client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		_, err = client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevision{
				Revision: 4,
			},
			[]event_streams.ProposedEvent{events[0]})
		require.Equal(t, event_streams.WrongExpectedVersionErr, err.Code())
	})

	t.Run("AppendMultipleEvents_sequence_0em1_0e0_non_idempotent", func(t *testing.T) {
		streamName := "AppendMultipleEvents_sequence_0em1_0e0_non_idempotent"

		events := testCreateEvents(1)

		_, err := client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		_, err = client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevision{
				Revision: 0,
			},
			[]event_streams.ProposedEvent{events[0]})
		require.NoError(t, err)

		readEvents, err := client.ReadStreamEvents(context.Background(),
			streamName,
			event_streams.ReadDirectionForward,
			event_streams.ReadStreamRevisionStart{},
			uint64(len(events)+2),
			false)
		require.NoError(t, err)
		require.Len(t, readEvents, len(events)+1)
	})

	t.Run("AppendMultipleEvents_sequence_0em1_0any_idempotent", func(t *testing.T) {
		streamName := "AppendMultipleEvents_sequence_0em1_0any_idempotent"

		events := testCreateEvents(1)

		_, err := client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		_, err = client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionAny{},
			[]event_streams.ProposedEvent{events[0]})
		require.NoError(t, err)

		readEvents, err := client.ReadStreamEvents(context.Background(),
			streamName,
			event_streams.ReadDirectionForward,
			event_streams.ReadStreamRevisionStart{},
			uint64(len(events)+1),
			false)
		require.NoError(t, err)
		require.Len(t, readEvents, len(events))
	})

	t.Run("AppendMultipleEvents_sequence_0em1_0em1_idempotent", func(t *testing.T) {
		streamName := "AppendMultipleEvents_sequence_0em1_0em1_idempotent"

		events := testCreateEvents(1)

		_, err := client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		_, err = client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionNoStream{},
			[]event_streams.ProposedEvent{events[0]})
		require.NoError(t, err)

		readEvents, err := client.ReadStreamEvents(context.Background(),
			streamName,
			event_streams.ReadDirectionForward,
			event_streams.ReadStreamRevisionStart{},
			uint64(len(events)+1),
			false)
		require.NoError(t, err)
		require.Len(t, readEvents, len(events))
	})

	t.Run("AppendMultipleEvents_sequence_0em1_1e0_2e1_1any_1any_idempotent", func(t *testing.T) {
		streamName := "AppendMultipleEvents_sequence_0em1_1e0_2e1_1any_1any_idempotent"

		events := testCreateEvents(3)

		_, err := client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		_, err = client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionAny{},
			[]event_streams.ProposedEvent{events[1]})
		require.NoError(t, err)

		_, err = client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionAny{},
			[]event_streams.ProposedEvent{events[1]})
		require.NoError(t, err)

		readEvents, err := client.ReadStreamEvents(context.Background(),
			streamName,
			event_streams.ReadDirectionForward,
			event_streams.ReadStreamRevisionStart{},
			uint64(len(events)+1),
			false)
		require.NoError(t, err)
		require.Len(t, readEvents, len(events))
	})

	t.Run("AppendMultipleEvents_sequence_S_0em1_1em1_E_S_0em1_E_idempotent", func(t *testing.T) {
		streamName := "AppendMultipleEvents_sequence_S_0em1_1em1_E_S_0em1_E_idempotent"

		events := testCreateEvents(2)

		_, err := client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		_, err = client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionNoStream{},
			[]event_streams.ProposedEvent{events[0]})
		require.NoError(t, err)

		readEvents, err := client.ReadStreamEvents(context.Background(),
			streamName,
			event_streams.ReadDirectionForward,
			event_streams.ReadStreamRevisionStart{},
			uint64(len(events)+1),
			false)
		require.NoError(t, err)
		require.Len(t, readEvents, len(events))
	})

	t.Run("AppendMultipleEvents_sequence_S_0em1_1em1_E_S_0any_E_idempotent", func(t *testing.T) {
		streamName := "AppendMultipleEvents_sequence_S_0em1_1em1_E_S_0any_E_idempotent"

		events := testCreateEvents(2)

		_, err := client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		_, err = client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionAny{},
			[]event_streams.ProposedEvent{events[0]})
		require.NoError(t, err)

		readEvents, err := client.ReadStreamEvents(context.Background(),
			streamName,
			event_streams.ReadDirectionForward,
			event_streams.ReadStreamRevisionStart{},
			uint64(len(events)+1),
			false)
		require.NoError(t, err)
		require.Len(t, readEvents, len(events))
	})

	t.Run("AppendMultipleEvents_sequence_S_0em1_1em1_E_S_1e0_E_idempotent", func(t *testing.T) {
		streamName := "AppendMultipleEvents_sequence_S_0em1_1em1_E_S_1e0_E_idempotent"

		events := testCreateEvents(2)

		_, err := client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		_, err = client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevision{
				Revision: 0,
			},
			[]event_streams.ProposedEvent{events[1]})
		require.NoError(t, err)

		readEvents, err := client.ReadStreamEvents(context.Background(),
			streamName,
			event_streams.ReadDirectionForward,
			event_streams.ReadStreamRevisionStart{},
			uint64(len(events)+1),
			false)
		require.NoError(t, err)
		require.Len(t, readEvents, len(events))
	})

	t.Run("AppendMultipleEvents_sequence_S_0em1_1em1_E_S_1any_E_idempotent", func(t *testing.T) {
		streamName := "AppendMultipleEvents_sequence_S_0em1_1em1_E_S_1any_E_idempotent"

		events := testCreateEvents(2)

		_, err := client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionNoStream{},
			events)
		require.NoError(t, err)

		_, err = client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionAny{},
			[]event_streams.ProposedEvent{events[1]})
		require.NoError(t, err)

		readEvents, err := client.ReadStreamEvents(context.Background(),
			streamName,
			event_streams.ReadDirectionForward,
			event_streams.ReadStreamRevisionStart{},
			uint64(len(events)+1),
			false)
		require.NoError(t, err)
		require.Len(t, readEvents, len(events))
	})

	t.Run("AppendMultipleEvents_sequence_S_0em1_1em1_E_S_0em1_1em1_2em1_E_idempotency_return_wev", func(t *testing.T) {
		streamName := "AppendMultipleEvents_sequence_S_0em1_1em1_E_S_0em1_1em1_2em1_E_idempotency_return_wev"

		events := testCreateEvents(3)

		_, err := client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionNoStream{},
			events[:2])
		require.NoError(t, err)

		_, err = client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionNoStream{},
			events)
		require.Equal(t, event_streams.WrongExpectedVersionErr, err.Code())
	})
}

func Test_Append_WithIncorrectCredentials(t *testing.T) {
	client, closeFunc := initializeContainerAndClientWithCredentials(t,
		"wrong_user_name", "wrong_password", nil)
	defer closeFunc()
	events := testCreateEvents(3)

	streamName := "Append_WithIncorrectCredentials"
	_, err := client.AppendToStream(context.Background(),
		streamName,
		event_streams.WriteStreamRevisionNoStream{},
		events[:2])
	require.Equal(t, errors.UnauthenticatedErr, err.Code())
}
