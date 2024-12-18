package kurrent_test

import (
	"context"
	"testing"

	"github.com/EventStore/EventStore-Client-Go/v4/kurrent"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func DeleteTests(t *testing.T, db *kurrent.Client) {
	t.Run("DeleteTests", func(t *testing.T) {
		t.Run("canDeleteStream", canDeleteStream(db))
		t.Run("canTombstoneStream", canTombstoneStream(db))
		t.Run("detectStreamDeleted", detectStreamDeleted(db))
	})
}
func canDeleteStream(db *kurrent.Client) TestCall {
	return func(t *testing.T) {
		opts := kurrent.DeleteStreamOptions{
			ExpectedRevision: kurrent.Revision(0),
		}

		streamID := NAME_GENERATOR.Generate()

		_, err := db.AppendToStream(context.Background(), streamID, kurrent.AppendToStreamOptions{}, createTestEvent())
		assert.NoError(t, err)
		deleteResult, err := db.DeleteStream(context.Background(), streamID, opts)

		if err != nil {
			t.Fatalf("Unexpected failure %+v", err)
		}

		assert.True(t, deleteResult.Position.Commit > 0)
		assert.True(t, deleteResult.Position.Prepare > 0)
	}
}

func canTombstoneStream(db *kurrent.Client) TestCall {
	return func(t *testing.T) {
		streamId := NAME_GENERATOR.Generate()

		_, err := db.AppendToStream(context.Background(), streamId, kurrent.AppendToStreamOptions{}, createTestEvent())
		deleteResult, err := db.TombstoneStream(context.Background(), streamId, kurrent.TombstoneStreamOptions{
			ExpectedRevision: kurrent.Revision(0),
		})

		if err != nil {
			t.Fatalf("Unexpected failure %+v", err)
		}

		assert.True(t, deleteResult.Position.Commit > 0)
		assert.True(t, deleteResult.Position.Prepare > 0)

		_, err = db.AppendToStream(context.Background(), streamId, kurrent.AppendToStreamOptions{}, createTestEvent())
		require.Error(t, err)
	}
}

func detectStreamDeleted(db *kurrent.Client) TestCall {
	return func(t *testing.T) {
		streamID := NAME_GENERATOR.Generate()
		event := createTestEvent()

		_, err := db.AppendToStream(context.Background(), streamID, kurrent.AppendToStreamOptions{}, event)
		require.Nil(t, err)

		_, err = db.TombstoneStream(context.Background(), streamID, kurrent.TombstoneStreamOptions{})
		require.Nil(t, err)

		stream, err := db.ReadStream(context.Background(), streamID, kurrent.ReadStreamOptions{}, 1)
		require.NoError(t, err)
		defer stream.Close()

		evt, err := stream.Recv()
		require.Nil(t, evt)
		esdbErr, ok := kurrent.FromError(err)
		require.False(t, ok)
		require.Equal(t, esdbErr.Code(), kurrent.ErrorCodeStreamDeleted)
	}
}
