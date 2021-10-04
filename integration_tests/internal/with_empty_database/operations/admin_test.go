package operations_integration_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_MergeIndexes(t *testing.T) {
	client, closeFunc := initializeClient(t, nil)
	defer closeFunc()

	err := client.MergeIndexes(context.Background())
	require.NoError(t, err)
}

func Test_RestartPersistentSubscriptions(t *testing.T) {
	client, closeFunc := initializeClient(t, nil)
	defer closeFunc()

	err := client.RestartPersistentSubscriptions(context.Background())
	require.NoError(t, err)
}

func Test_ResignNode(t *testing.T) {
	client, closeFunc := initializeClient(t, nil)
	defer closeFunc()

	err := client.ResignNode(context.Background())
	require.NoError(t, err)
}

func Test_Shutdown(t *testing.T) {
	client, closeFunc := initializeClient(t, nil)
	defer closeFunc()

	err := client.Shutdown(context.Background())
	require.NoError(t, err)
}
