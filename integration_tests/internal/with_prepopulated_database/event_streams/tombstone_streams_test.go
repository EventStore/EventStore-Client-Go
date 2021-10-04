package event_streams_with_prepopulated_database

import (
	"context"
	"testing"

	"github.com/pivonroll/EventStore-Client-Go/errors"

	"github.com/stretchr/testify/require"

	"github.com/pivonroll/EventStore-Client-Go/event_streams"
)

func Test_TombstoneStream(t *testing.T) {
	client, closeFunc := initializeWithPrePopulatedDatabase(t)
	defer closeFunc()

	result, err := client.TombstoneStream(context.Background(),
		"dataset20M-1800",
		event_streams.TombstoneRequestExpectedStreamRevision{Revision: 1999})
	require.NoError(t, err)

	position, isPosition := result.GetPosition()
	require.True(t, isPosition)
	require.True(t, position.PreparePosition > 0)
	require.True(t, position.CommitPosition > 0)

	_, err = client.AppendToStream(context.Background(),
		"dataset20M-1800",
		event_streams.AppendRequestExpectedStreamRevisionAny{},
		testCreateEvents(1))
	require.Equal(t, errors.StreamDeletedErr, err.Code())
}
