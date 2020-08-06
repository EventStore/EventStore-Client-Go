package client_test

import (
	"context"
	"testing"

	"github.com/EventStore/EventStore-Client-Go/streamrevision"
	"github.com/stretchr/testify/assert"
)

func TestDeletes(t *testing.T) {
	container := GetPrePopulatedDatabase()
	defer container.Close()

	t.Run("TestCanDeleteStream", func(t *testing.T) {
		client := CreateTestClient(container, t)
		defer client.Close()

		deleteResult, err := client.SoftDeleteStream(context.Background(), "dataset20M-1800", streamrevision.NewStreamRevision(1999))

		if err != nil {
			t.Fatalf("Unexpected failure %+v", err)
		}

		assert.True(t, deleteResult.CommitPosition > 0)
		assert.True(t, deleteResult.PreparePosition > 0)
	})
}
