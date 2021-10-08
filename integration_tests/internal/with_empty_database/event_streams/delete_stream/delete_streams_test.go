package delete_stream

import (
	"context"
	"testing"
	"time"

	"github.com/pivonroll/EventStore-Client-Go/ptr"

	"github.com/pivonroll/EventStore-Client-Go/errors"
	"github.com/stretchr/testify/require"

	"github.com/pivonroll/EventStore-Client-Go/event_streams"
)

func Test_DeleteStream_WithTimeout(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	t.Run("Any stream", func(t *testing.T) {
		streamName := "delete_any_stream"

		ctx := context.Background()
		timeoutCtx, cancelFunc := context.WithTimeout(ctx, 0)
		_, err := client.DeleteStream(timeoutCtx,
			streamName,
			event_streams.WriteStreamRevisionAny{})
		require.Equal(t, errors.DeadlineExceededErr, err.Code())

		defer cancelFunc()
	})

	t.Run("Any stream", func(t *testing.T) {
		streamName := "delete_stream_revision_0"

		ctx := context.Background()
		timeoutCtx, cancelFunc := context.WithTimeout(ctx, 0)
		_, err := client.DeleteStream(timeoutCtx,
			streamName,
			event_streams.WriteStreamRevision{
				Revision: 0,
			})
		require.Equal(t, errors.DeadlineExceededErr, err.Code())

		defer cancelFunc()
	})
}

func Test_DeleteStream(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t, nil)
	defer closeFunc()

	t.Run("Stream Does Not Exist, Revision NoStream", func(t *testing.T) {
		streamName := "stream_does_not_exist_no_stream"

		_, err := client.DeleteStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionNoStream{})
		require.NoError(t, err)
	})

	t.Run("Stream Does Not Exist, Revision Any", func(t *testing.T) {
		streamName := "stream_does_not_exist_any"

		_, err := client.DeleteStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionAny{})
		require.NoError(t, err)
	})

	t.Run("Stream Does Not Exist, Wrong Revision", func(t *testing.T) {
		streamName := "stream_does_not_exist_wrong_version"

		_, err := client.DeleteStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevision{Revision: 0})
		require.Equal(t, errors.WrongExpectedStreamRevisionErr, err.Code())
	})

	t.Run("Stream with events returns position", func(t *testing.T) {
		streamName := "stream_with_events"

		event := testCreateEvent()

		writeResult, err := client.AppendToStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevisionNoStream{},
			[]event_streams.ProposedEvent{event})
		require.NoError(t, err)

		deleteResult, err := client.DeleteStream(context.Background(),
			streamName,
			event_streams.WriteStreamRevision{Revision: writeResult.GetCurrentRevision()})
		require.NoError(t, err)

		tombstonePosition, isTombstonePosition := deleteResult.GetPosition()
		require.True(t, isTombstonePosition)
		writePosition, isWritePosition := writeResult.GetPosition()
		require.True(t, isWritePosition)

		require.True(t, tombstonePosition.GreaterThan(writePosition))
	})

	type AppendAfterDeleteAnyNoStream struct {
		name     string
		revision event_streams.IsWriteStreamRevision
	}

	revisions := []AppendAfterDeleteAnyNoStream{
		{
			name:     "Any",
			revision: event_streams.WriteStreamRevisionAny{},
		},
		{
			name:     "No Stream",
			revision: event_streams.WriteStreamRevisionNoStream{},
		},
	}

	for _, revision := range revisions {
		t.Run("Recreated After Deleted With "+revision.name, func(t *testing.T) {
			streamId := "append_after_deleted_with_" + revision.name

			writeResult, err := client.AppendToStream(context.Background(),
				streamId,
				event_streams.WriteStreamRevisionNoStream{},
				testCreateEvents(1))
			require.NoError(t, err)

			_, err = client.DeleteStream(context.Background(),
				streamId,
				event_streams.WriteStreamRevision{Revision: writeResult.GetCurrentRevision()})
			require.NoError(t, err)

			events := testCreateEvents(3)

			writeResult, err = client.AppendToStream(context.Background(),
				streamId,
				revision.revision, // Focus of the test
				events)
			require.NoError(t, err)

			require.EqualValues(t, 3, writeResult.GetCurrentRevision())

			readEvents, err := client.ReadStreamEvents(context.Background(),
				streamId,
				event_streams.ReadDirectionForward,
				event_streams.ReadStreamRevisionStart{},
				event_streams.ReadCountMax,
				false)
			require.NoError(t, err)

			require.Len(t, readEvents, 3)
			require.Equal(t, events, readEvents.ToProposedEvents())

			metaData, err := client.GetStreamMetadata(context.Background(), streamId)
			require.NoError(t, err)

			streamMetadata := metaData.GetStreamMetadata()
			require.EqualValues(t, 1, *streamMetadata.TruncateBefore)
			require.EqualValues(t, 1, metaData.GetMetaStreamRevision())
		})
	}

	t.Run("Recreated After Deleted With Expected Stream Revision", func(t *testing.T) {
		streamId := "append_after_deleted_with_exact_revision"

		writeResult, err := client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionNoStream{},
			testCreateEvents(1))
		require.NoError(t, err)

		_, err = client.DeleteStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevision{Revision: writeResult.GetCurrentRevision()})
		require.NoError(t, err)

		events := testCreateEvents(3)

		writeResult, err = client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevision{ // Focus of the test
				Revision: writeResult.GetCurrentRevision(),
			},
			events)
		require.NoError(t, err)

		require.EqualValues(t, 3, writeResult.GetCurrentRevision())

		readEvents, err := client.ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadDirectionForward,
			event_streams.ReadStreamRevisionStart{},
			event_streams.ReadCountMax,
			false)
		require.NoError(t, err)
		require.Len(t, readEvents, 3)
		require.Equal(t, events, readEvents.ToProposedEvents())

		metaData, err := client.GetStreamMetadata(context.Background(), streamId)
		require.NoError(t, err)

		streamMetadata := metaData.GetStreamMetadata()
		require.EqualValues(t, 1, *streamMetadata.TruncateBefore)
		require.EqualValues(t, 1, metaData.GetMetaStreamRevision())
	})

	t.Run("Recreated Preserves Metadata Except truncated Before", func(t *testing.T) {
		t.Skip("Skipped because it does not behave as equivalent DotNet test recreated_preserves_metadata_except_truncate_before")
		streamId := "recreated_preserves_metadata_except_truncate_before"

		writeResult, err := client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionNoStream{},
			testCreateEvents(2))
		require.NoError(t, err)
		require.EqualValues(t, 1, writeResult.GetCurrentRevision())

		truncateBefore := event_streams.ReadCountMax - 1
		streamMetadata := event_streams.StreamMetadata{
			MaxAgeInSeconds:       nil,
			TruncateBefore:        &truncateBefore,
			CacheControlInSeconds: nil,
			Acl: &event_streams.StreamAcl{
				DeleteRoles: []string{"some-role"},
			},
			MaxCount: ptr.Int(100),
			CustomMetadata: event_streams.CustomMetadataType{
				"key1": true,
				"key2": 17,
				"key3": "some value",
			},
		}

		streamMetadataResponse, err := client.SetStreamMetadata(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionNoStream{},
			streamMetadata)
		require.NoError(t, err)
		require.EqualValues(t, 0, streamMetadataResponse.GetCurrentRevision())

		events := testCreateEvents(3)
		writeResult, err = client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevision{ // Focus of the test
				Revision: 1,
			},
			events)
		require.NoError(t, err)

		readEvents, err := client.ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadDirectionForward,
			event_streams.ReadStreamRevisionStart{},
			event_streams.ReadCountMax,
			false)
		require.NoError(t, err)
		require.Len(t, readEvents, 3)
		require.Equal(t, events, readEvents.ToProposedEvents())

		metaData, err := client.GetStreamMetadata(context.Background(), streamId)
		require.NoError(t, err)

		expectedStreamMetadata := streamMetadata
		*expectedStreamMetadata.TruncateBefore = 2
		require.EqualValues(t, 2, metaData.GetMetaStreamRevision())
		require.Equal(t, expectedStreamMetadata, metaData.GetStreamMetadata())
	})

	t.Run("Soft Deleted Stream Can Be Hard Deleted", func(t *testing.T) {
		streamId := "can_be_hard_deleted"
		writeResult, err := client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionNoStream{},
			testCreateEvents(2))
		require.NoError(t, err)
		require.EqualValues(t, 1, writeResult.GetCurrentRevision())

		_, err = client.DeleteStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevision{Revision: writeResult.GetCurrentRevision()})
		require.NoError(t, err)

		_, err = client.TombstoneStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionAny{})
		require.NoError(t, err)

		_, err = client.ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadDirectionForward,
			event_streams.ReadStreamRevisionStart{},
			event_streams.ReadCountMax,
			false)
		require.Equal(t, errors.StreamDeletedErr, err.Code())

		_, err = client.GetStreamMetadata(context.Background(), streamId)
		require.Equal(t, errors.StreamDeletedErr, err.Code())

		_, err = client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionAny{},
			testCreateEvents(1))
		require.Equal(t, errors.StreamDeletedErr, err.Code())
	})

	t.Run("Allows Recreating For First Write Only", func(t *testing.T) {
		streamId := "allows_recreating_for_first_write_only"
		writeResult, err := client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionNoStream{},
			testCreateEvents(2))
		require.NoError(t, err)
		require.EqualValues(t, 1, writeResult.GetCurrentRevision())

		_, err = client.DeleteStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevision{Revision: writeResult.GetCurrentRevision()})
		require.NoError(t, err)

		writeResult, err = client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionNoStream{},
			testCreateEvents(3))
		require.NoError(t, err)
		require.EqualValues(t, 4, writeResult.GetCurrentRevision())

		writeResult, err = client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionNoStream{},
			testCreateEvents(1))
		require.Equal(t, event_streams.WrongExpectedVersionErr, err.Code())
	})

	t.Run("Appends Multiple Writes Expected Version Any", func(t *testing.T) {
		streamId := "appends_multiple_writes_expected_version_any"
		writeResult, err := client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionNoStream{},
			testCreateEvents(2))
		require.NoError(t, err)
		require.EqualValues(t, 1, writeResult.GetCurrentRevision())

		_, err = client.DeleteStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevision{Revision: writeResult.GetCurrentRevision()})
		require.NoError(t, err)

		firstEvents := testCreateEvents(3)

		writeResult, err = client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionAny{},
			firstEvents)
		require.NoError(t, err)
		require.EqualValues(t, 4, writeResult.GetCurrentRevision())

		secondEvents := testCreateEvents(2)
		writeResult, err = client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionAny{},
			secondEvents)
		require.NoError(t, err)
		require.EqualValues(t, 6, writeResult.GetCurrentRevision())

		readEvents, err := client.ReadStreamEvents(context.Background(),
			streamId,
			event_streams.ReadDirectionForward,
			event_streams.ReadStreamRevisionStart{},
			event_streams.ReadCountMax,
			false)
		require.NoError(t, err)
		require.Equal(t, append(firstEvents, secondEvents...), readEvents.ToProposedEvents())

		metaData, err := client.GetStreamMetadata(context.Background(), streamId)
		require.NoError(t, err)
		streamMetadata := metaData.GetStreamMetadata()
		require.EqualValues(t, 2, *streamMetadata.TruncateBefore)
		require.EqualValues(t, 1, metaData.GetMetaStreamRevision())
	})

	t.Run("Recreated On Empty When Metadata Set", func(t *testing.T) {
		streamId := "recreated_on_empty_when_metadata_set"

		_, err := client.DeleteStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionNoStream{})
		require.NoError(t, err)

		truncateBefore := event_streams.ReadCountMax
		streamMetadata := event_streams.StreamMetadata{
			MaxAgeInSeconds:       nil,
			TruncateBefore:        &truncateBefore,
			CacheControlInSeconds: nil,
			Acl: &event_streams.StreamAcl{
				DeleteRoles: []string{"some-role"},
			},
			MaxCount: ptr.Int(100),
			CustomMetadata: event_streams.CustomMetadataType{
				"key1": true,
				"key2": float64(17),
				"key3": "some value",
			},
		}

		streamMetadataResponse, err := client.SetStreamMetadata(context.Background(),
			streamId,
			event_streams.WriteStreamRevision{Revision: 0},
			streamMetadata)
		require.NoError(t, err)
		require.EqualValues(t, 1, streamMetadataResponse.GetCurrentRevision())

		require.Eventually(t, func() bool {
			_, err = client.ReadStreamEvents(context.Background(),
				streamId,
				event_streams.ReadDirectionForward,
				event_streams.ReadStreamRevisionStart{},
				event_streams.ReadCountMax,
				false)
			return err != nil && err.Code() == errors.StreamNotFoundErr
		}, time.Second*5, time.Millisecond*200)

		expectedMetaData := streamMetadata
		*expectedMetaData.TruncateBefore = 0
		metaData, err := client.GetStreamMetadata(context.Background(), streamId)
		require.NoError(t, err)
		require.EqualValues(t, 2, metaData.GetMetaStreamRevision())
		require.Equal(t, expectedMetaData, metaData.GetStreamMetadata())
	})

	t.Run("Recreated On Non Empty When Metadata Set", func(t *testing.T) {
		t.Skip("Skipped because metadata write recreates a stream with all events")
		streamId := "recreated_on_non_empty_when_metadata_set"

		writeResult, err := client.AppendToStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevisionNoStream{},
			testCreateEvents(2))
		require.NoError(t, err)
		require.EqualValues(t, 1, writeResult.GetCurrentRevision())

		_, err = client.DeleteStream(context.Background(),
			streamId,
			event_streams.WriteStreamRevision{Revision: writeResult.GetCurrentRevision()})
		require.NoError(t, err)

		streamMetadata := event_streams.StreamMetadata{
			MaxAgeInSeconds:       nil,
			TruncateBefore:        nil,
			CacheControlInSeconds: nil,
			Acl: &event_streams.StreamAcl{
				DeleteRoles: []string{"some-role"},
			},
			MaxCount: ptr.Int(100),
			CustomMetadata: event_streams.CustomMetadataType{
				"key1": true,
				"key2": float64(17),
				"key3": "some value",
			},
		}

		streamMetadataResponse, err := client.SetStreamMetadata(context.Background(),
			streamId,
			event_streams.WriteStreamRevision{Revision: 0},
			streamMetadata)
		require.NoError(t, err)
		require.EqualValues(t, 1, streamMetadataResponse.GetCurrentRevision())

		require.Eventually(t, func() bool {
			readEvents, err := client.ReadStreamEvents(context.Background(),
				streamId,
				event_streams.ReadDirectionForward,
				event_streams.ReadStreamRevisionStart{},
				event_streams.ReadCountMax,
				false)
			require.NoError(t, err)

			return err == nil && len(readEvents) == 0
		}, time.Second*10, time.Millisecond*500)

		expectedMetaData := streamMetadata
		*expectedMetaData.TruncateBefore = 2
		metaData, err := client.GetStreamMetadata(context.Background(), streamId)
		require.NoError(t, err)
		require.Equal(t, expectedMetaData, metaData.GetStreamMetadata())
	})
}

func Test_DeleteStream_WithIncorrectCredentials(t *testing.T) {
	client, closeFunc := initializeContainerAndClientWithCredentials(t,
		"wrong_user_name", "wrong_password", nil)
	defer closeFunc()

	streamName := "stream_does_not_exist_no_stream"

	_, err := client.DeleteStream(context.Background(),
		streamName,
		event_streams.WriteStreamRevisionNoStream{})
	require.Equal(t, errors.UnauthenticatedErr, err.Code())
}
