package operations_integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/pivonroll/EventStore-Client-Go/errors"
	"github.com/pivonroll/EventStore-Client-Go/operations"
	"github.com/stretchr/testify/require"
)

func Test_InvalidScavengeParameters(t *testing.T) {
	client, closeFunc := initializeClient(t, nil)
	defer closeFunc()

	t.Run("Invalid Thread Count", func(t *testing.T) {
		_, err := client.StartScavenge(context.Background(), operations.StartScavengeRequest{
			ThreadCount:    0,
			StartFromChunk: 0,
		})
		require.Equal(t, operations.StartScavenge_ThreadCountLessOrEqualZeroErr, err.Code())

		_, err = client.StartScavenge(context.Background(), operations.StartScavengeRequest{
			ThreadCount:    -1,
			StartFromChunk: 0,
		})
		require.Equal(t, operations.StartScavenge_ThreadCountLessOrEqualZeroErr, err.Code())
	})

	t.Run("Invalid Chunk Size", func(t *testing.T) {
		_, err := client.StartScavenge(context.Background(), operations.StartScavengeRequest{
			ThreadCount:    1,
			StartFromChunk: -1,
		})
		require.Equal(t, operations.StartScavenge_StartFromChunkLessThanZeroErr, err.Code())
	})

	t.Run("Start Scavenge", func(t *testing.T) {
		response, err := client.StartScavenge(context.Background(), operations.StartScavengeRequest{
			ThreadCount:    1,
			StartFromChunk: 0,
		})

		require.NoError(t, err)
		require.Equal(t, operations.ScavengeStatus_Started, response.Status)
	})
}

func Test_StartScavenge(t *testing.T) {
	client, closeFunc := initializeClient(t, nil)
	defer closeFunc()

	t.Run("Start Scavenge", func(t *testing.T) {
		response, err := client.StartScavenge(context.Background(), operations.StartScavengeRequest{
			ThreadCount:    1,
			StartFromChunk: 0,
		})

		require.NoError(t, err)
		require.Equal(t, operations.ScavengeStatus_Started, response.Status)
	})
}

func Test_StartAndStopScavenge(t *testing.T) {
	client, closeFunc := initializeClient(t, nil)
	defer closeFunc()

	response, err := client.StartScavenge(context.Background(), operations.StartScavengeRequest{
		ThreadCount:    1,
		StartFromChunk: 0,
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		result, err := client.StopScavenge(context.Background(), response.ScavengeId)
		if err.Code() == errors.ScavengeNotFoundErr {
			fmt.Println("On empty database scavenge finishes too quickly.")
			return true
		}
		return err == nil && operations.ScavengeStatus_Stopped == result.Status
	}, time.Second*10, time.Second)
}

func Test_StopScavenge_WhenNoScavengeIsRunning(t *testing.T) {
	client, closeFunc := initializeClient(t, nil)
	defer closeFunc()

	t.Run("Stop Scavenge WhenNoScavengeIsRunning", func(t *testing.T) {
		scavengeId, _ := uuid.NewV4()
		_, err := client.StopScavenge(context.Background(), scavengeId.String())
		require.Equal(t, errors.ScavengeNotFoundErr, err.Code())
	})
}
