package esdb_test

import (
	"context"
	"testing"

	esdb2 "github.com/EventStore/EventStore-Client-Go/v2/esdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func DeleteTests(t *testing.T, db *esdb2.Client) {
	t.Run("DeleteTests", func(t *testing.T) {
		t.Run("canDeleteStream", canDeleteStream(db))
		t.Run("canTombstoneStream", canTombstoneStream(db))
		t.Run("detectStreamDeleted", detectStreamDeleted(db))
	})
}
func canDeleteStream(db *esdb2.Client) TestCall {
	return func(t *testing.T) {
		opts := esdb2.DeleteStreamOptions{
			ExpectedRevision: esdb2.Revision(0),
		}

		streamID := NAME_GENERATOR.Generate()

		_, err := db.AppendToStream(context.Background(), streamID, esdb2.AppendToStreamOptions{}, createTestEvent())
		assert.NoError(t, err)
		deleteResult, err := db.DeleteStream(context.Background(), streamID, opts)

		if err != nil {
			t.Fatalf("Unexpected failure %+v", err)
		}

		assert.True(t, deleteResult.Position.Commit > 0)
		assert.True(t, deleteResult.Position.Prepare > 0)
	}
}

func canTombstoneStream(db *esdb2.Client) TestCall {
	return func(t *testing.T) {
		streamId := NAME_GENERATOR.Generate()

		_, err := db.AppendToStream(context.Background(), streamId, esdb2.AppendToStreamOptions{}, createTestEvent())
		deleteResult, err := db.TombstoneStream(context.Background(), streamId, esdb2.TombstoneStreamOptions{
			ExpectedRevision: esdb2.Revision(0),
		})

		if err != nil {
			t.Fatalf("Unexpected failure %+v", err)
		}

		assert.True(t, deleteResult.Position.Commit > 0)
		assert.True(t, deleteResult.Position.Prepare > 0)

		_, err = db.AppendToStream(context.Background(), streamId, esdb2.AppendToStreamOptions{}, createTestEvent())
		require.Error(t, err)
	}
}

func detectStreamDeleted(db *esdb2.Client) TestCall {
	return func(t *testing.T) {
		streamID := NAME_GENERATOR.Generate()
		event := createTestEvent()

		_, err := db.AppendToStream(context.Background(), streamID, esdb2.AppendToStreamOptions{}, event)
		require.Nil(t, err)

		_, err = db.TombstoneStream(context.Background(), streamID, esdb2.TombstoneStreamOptions{})
		require.Nil(t, err)

		_, err = db.ReadStream(context.Background(), streamID, esdb2.ReadStreamOptions{}, 1)
		esdbErr, ok := esdb2.FromError(err)
		assert.False(t, ok)
		assert.Equal(t, esdbErr.Code(), esdb2.ErrorStreamDeleted)
	}
}
