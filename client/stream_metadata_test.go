package client_test

import (
	"context"
	"testing"

	"github.com/pivonroll/EventStore-Client-Go/errors"
	"github.com/pivonroll/EventStore-Client-Go/event_streams"
	"github.com/pivonroll/EventStore-Client-Go/ptr"
	"github.com/stretchr/testify/require"
)

func Test_StreamMetaData(t *testing.T) {
	container := getEmptyDatabase()
	defer container.Close()
	client := createClientConnectedToContainer(container, t)
	defer func() {
		err := client.Close()
		if err != nil {
			panic(err)
		}
	}()

	t.Run("getting_for_an_existing_stream_and_no_metadata_exists", func(t *testing.T) {
		streamId := "getting_for_an_existing_stream_and_no_metadata_exists"

		_, err := client.EventStreams().AppendToStream(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			testCreateEvents(2))
		require.NoError(t, err)

		metaData, err := client.EventStreams().GetStreamMetadata(context.Background(), streamId)
		require.NoError(t, err)
		require.True(t, metaData.IsNone())
	})

	t.Run("empty_metadata", func(t *testing.T) {
		streamId := "empty_metadata"
		_, err := client.EventStreams().SetStreamMetadata(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			event_streams.StreamMetadata{},
		)
		require.NoError(t, err)

		metaData, err := client.EventStreams().GetStreamMetadata(context.Background(), streamId)
		require.NoError(t, err)
		require.False(t, metaData.IsNone())
		require.Equal(t, streamId, metaData.GetStreamId())
		require.EqualValues(t, 0, metaData.GetMetaStreamRevision())
		require.Equal(t, event_streams.StreamMetadata{}, metaData.GetStreamMetadata())
	})

	t.Run("latest_metadata_is_returned", func(t *testing.T) {
		streamId := "latest_metadata_is_returned"

		expectedStreamMetadata := event_streams.StreamMetadata{
			MaxCount:              ptr.Int(17),
			TruncateBefore:        ptr.UInt64(10),
			CacheControlInSeconds: ptr.UInt64(17),
			MaxAgeInSeconds:       ptr.UInt64(15),
		}

		_, err := client.EventStreams().SetStreamMetadata(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevisionNoStream{},
			expectedStreamMetadata,
		)
		require.NoError(t, err)
		metaData, err := client.EventStreams().GetStreamMetadata(context.Background(), streamId)
		require.NoError(t, err)
		require.False(t, metaData.IsNone())
		require.EqualValues(t, 0, metaData.GetMetaStreamRevision())
		require.Equal(t, expectedStreamMetadata, metaData.GetStreamMetadata())

		expectedStreamMetadata = event_streams.StreamMetadata{
			MaxCount:              ptr.Int(25),
			TruncateBefore:        ptr.UInt64(20),
			CacheControlInSeconds: ptr.UInt64(7),
			MaxAgeInSeconds:       ptr.UInt64(11),
		}

		_, err = client.EventStreams().SetStreamMetadata(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevision{Revision: 0},
			expectedStreamMetadata,
		)
		require.NoError(t, err)
		metaData, err = client.EventStreams().GetStreamMetadata(context.Background(), streamId)
		require.NoError(t, err)
		require.False(t, metaData.IsNone())
		require.EqualValues(t, 1, metaData.GetMetaStreamRevision())
		require.Equal(t, expectedStreamMetadata, metaData.GetStreamMetadata())
	})

	t.Run("setting_with_wrong_expected_version_throws", func(t *testing.T) {
		streamId := "setting_with_wrong_expected_version_throws"

		writeResult, err := client.EventStreams().SetStreamMetadata(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevision{Revision: 2},
			event_streams.StreamMetadata{},
		)
		require.NoError(t, err)
		_, isWrongExpectedVersion := writeResult.GetWrongExpectedVersion()
		require.True(t, isWrongExpectedVersion)
	})

	t.Run("latest_metadata_returned_stream_revision_any", func(t *testing.T) {
		streamId := "latest_metadata_returned_stream_revision_any"

		expectedStreamMetadata := event_streams.StreamMetadata{
			MaxCount:              ptr.Int(17),
			TruncateBefore:        ptr.UInt64(10),
			CacheControlInSeconds: ptr.UInt64(17),
			MaxAgeInSeconds:       ptr.UInt64(15),
		}

		_, err := client.EventStreams().SetStreamMetadata(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevisionAny{},
			expectedStreamMetadata,
		)
		require.NoError(t, err)
		metaData, err := client.EventStreams().GetStreamMetadata(context.Background(), streamId)
		require.NoError(t, err)
		require.False(t, metaData.IsNone())
		require.EqualValues(t, 0, metaData.GetMetaStreamRevision())
		require.Equal(t, expectedStreamMetadata, metaData.GetStreamMetadata())

		expectedStreamMetadata = event_streams.StreamMetadata{
			MaxCount:              ptr.Int(25),
			TruncateBefore:        ptr.UInt64(20),
			CacheControlInSeconds: ptr.UInt64(7),
			MaxAgeInSeconds:       ptr.UInt64(11),
		}

		_, err = client.EventStreams().SetStreamMetadata(context.Background(),
			streamId,
			event_streams.AppendRequestExpectedStreamRevisionAny{},
			expectedStreamMetadata,
		)
		require.NoError(t, err)
		metaData, err = client.EventStreams().GetStreamMetadata(context.Background(), streamId)
		require.NoError(t, err)
		require.False(t, metaData.IsNone())
		require.EqualValues(t, 1, metaData.GetMetaStreamRevision())
		require.Equal(t, expectedStreamMetadata, metaData.GetStreamMetadata())
	})

	t.Run("set_with_any_stream_revision_fails_when_operation_expired", func(t *testing.T) {
		streamId := "set_with_any_stream_revision_fails_when_operation_expired"
		ctx := context.Background()
		timeoutCtx, cancelFunc := context.WithTimeout(ctx, 0)
		defer cancelFunc()

		_, err := client.EventStreams().SetStreamMetadata(timeoutCtx,
			streamId,
			event_streams.AppendRequestExpectedStreamRevisionAny{},
			event_streams.StreamMetadata{},
		)
		require.Equal(t, errors.DeadlineExceededErr, err.Code())
	})

	t.Run("set_with_stream_revision_fails_when_operation_expired", func(t *testing.T) {
		streamId := "set_with_any_stream_revision_fails_when_operation_expired"
		ctx := context.Background()
		timeoutCtx, cancelFunc := context.WithTimeout(ctx, 0)
		defer cancelFunc()

		_, err := client.EventStreams().SetStreamMetadata(timeoutCtx,
			streamId,
			event_streams.AppendRequestExpectedStreamRevision{Revision: 0},
			event_streams.StreamMetadata{},
		)
		require.Equal(t, errors.DeadlineExceededErr, err.Code())
	})

	t.Run("get_fails_when_operation_expired", func(t *testing.T) {
		streamId := "set_with_any_stream_revision_fails_when_operation_expired"
		ctx := context.Background()
		timeoutCtx, cancelFunc := context.WithTimeout(ctx, 0)
		defer cancelFunc()

		_, err := client.EventStreams().GetStreamMetadata(timeoutCtx, streamId)
		require.Equal(t, errors.DeadlineExceededErr, err.Code())
	})
}
