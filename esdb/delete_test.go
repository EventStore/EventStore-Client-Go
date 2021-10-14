package esdb_test

import (
	"context"
	"errors"
	"testing"

	"github.com/EventStore/EventStore-Client-Go/esdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCanDeleteStream(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()

	db := CreateTestClient(container, t)
	defer db.Close()

	opts := esdb.DeleteStreamOptions{
		ExpectedRevision: esdb.Revision(1_999),
	}

	deleteResult, err := db.DeleteStream(context.Background(), "dataset20M-1800", opts)

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}

	assert.True(t, deleteResult.Position.Commit > 0)
	assert.True(t, deleteResult.Position.Prepare > 0)
}

func TestCanTombstoneStream(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()

	db := CreateTestClient(container, t)
	defer db.Close()

	deleteResult, err := db.TombstoneStream(context.Background(), "dataset20M-1800", esdb.TombstoneStreamOptions{
		ExpectedRevision: esdb.Revision(1_999),
	})

	if err != nil {
		t.Fatalf("Unexpected failure %+v", err)
	}

	assert.True(t, deleteResult.Position.Commit > 0)
	assert.True(t, deleteResult.Position.Prepare > 0)

	_, err = db.AppendToStream(context.Background(), "dataset20M-1800", esdb.AppendToStreamOptions{}, createTestEvent())
	require.Error(t, err)
}

func TestDetectStreamDeleted(t *testing.T) {
	container := GetEmptyDatabase()
	defer container.Close()

	db := CreateTestClient(container, t)
	defer db.Close()

	event := createTestEvent()

	_, err := db.AppendToStream(context.Background(), "foobar", esdb.AppendToStreamOptions{}, event)
	require.Nil(t, err)

	_, err = db.TombstoneStream(context.Background(), "foobar", esdb.TombstoneStreamOptions{})
	require.Nil(t, err)

	_, err = db.ReadStream(context.Background(), "foobar", esdb.ReadStreamOptions{}, 1)
	var streamDeletedError *esdb.StreamDeletedError

	require.True(t, errors.As(err, &streamDeletedError))
}
