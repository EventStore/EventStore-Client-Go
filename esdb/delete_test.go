package esdb_test

import (
	"context"
	"testing"

	"github.com/EventStore/EventStore-Client-Go/v2/esdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func DeleteTests(t *testing.T, db *esdb.Client) {
	t.Run("DeleteTests", func(t *testing.T) {
		t.Run("canDeleteStream", canDeleteStream(db))
		t.Run("canTombstoneStream", canTombstoneStream(db))
		t.Run("detectStreamDeleted", detectStreamDeleted(db))
	})
}
func canDeleteStream(db *esdb.Client) TestCall {
	return func(t *testing.T) {
		opts := esdb.DeleteStreamOptions{
			ExpectedRevision: esdb.Revision(0),
		}

		streamID := NAME_GENERATOR.Generate()

		_, err := db.AppendToStream(context.Background(), streamID, esdb.AppendToStreamOptions{}, createTestEvent())
		assert.NoError(t, err)
		deleteResult, err := db.DeleteStream(context.Background(), streamID, opts)

		if err != nil {
			t.Fatalf("Unexpected failure %+v", err)
		}

		assert.True(t, deleteResult.Position.Commit > 0)
		assert.True(t, deleteResult.Position.Prepare > 0)
	}
}

func canTombstoneStream(db *esdb.Client) TestCall {
	return func(t *testing.T) {
		streamId := NAME_GENERATOR.Generate()

		_, err := db.AppendToStream(context.Background(), streamId, esdb.AppendToStreamOptions{}, createTestEvent())
		deleteResult, err := db.TombstoneStream(context.Background(), streamId, esdb.TombstoneStreamOptions{
			ExpectedRevision: esdb.Revision(0),
		})

		if err != nil {
			t.Fatalf("Unexpected failure %+v", err)
		}

		assert.True(t, deleteResult.Position.Commit > 0)
		assert.True(t, deleteResult.Position.Prepare > 0)

		_, err = db.AppendToStream(context.Background(), streamId, esdb.AppendToStreamOptions{}, createTestEvent())
		require.Error(t, err)
	}
}

func detectStreamDeleted(db *esdb.Client) TestCall {
	return func(t *testing.T) {
		streamID := NAME_GENERATOR.Generate()
		event := createTestEvent()

		_, err := db.AppendToStream(context.Background(), streamID, esdb.AppendToStreamOptions{}, event)
		require.Nil(t, err)

		_, err = db.TombstoneStream(context.Background(), streamID, esdb.TombstoneStreamOptions{})
		require.Nil(t, err)

		stream, err := db.ReadStream(context.Background(), streamID, esdb.ReadStreamOptions{}, 1)
		require.NoError(t, err)
		defer stream.Close()

		evt, err := stream.Recv()
		require.Nil(t, evt)
		esdbErr, ok := esdb.FromError(err)
		require.False(t, ok)
		require.Equal(t, esdbErr.Code(), esdb.ErrorCodeStreamDeleted)
	}
}
