package event_streams_with_prepopulated_database

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/pivonroll/EventStore-Client-Go/event_streams"
)

func Test_DeleteStream(t *testing.T) {
	client, closeFunc := initializeWithPrePopulatedDatabase(t)
	defer closeFunc()

	result, err := client.DeleteStream(context.Background(),
		"dataset20M-1800",
		event_streams.WriteStreamRevision{Revision: 1999})
	require.NoError(t, err)

	position, isPosition := result.GetPosition()
	require.True(t, isPosition)
	require.True(t, position.PreparePosition > 0)
	require.True(t, position.CommitPosition > 0)
}
