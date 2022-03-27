package esdb_test

import (
	"context"
	"testing"

	"github.com/EventStore/EventStore-Client-Go/v2/esdb"

	"github.com/stretchr/testify/assert"
)

func ClusterTests(t *testing.T) {
	t.Run("ClusterTests", func(t *testing.T) {
		t.Run("notLeaderExceptionButWorkAfterRetry", notLeaderExceptionButWorkAfterRetry)
	})
}

func notLeaderExceptionButWorkAfterRetry(t *testing.T) {
	ctx := context.Background()

	// We purposely connect to a follower node so we can trigger on not leader exception.
	db := CreateClient("esdb://admin:changeit@localhost:2111,localhost:2112,localhost:2113?nodepreference=follower&tlsverifycert=false", t)
	defer db.Close()
	streamID := NAME_GENERATOR.Generate()

	err := db.CreatePersistentSubscription(ctx, streamID, "a_group", esdb.PersistentStreamSubscriptionOptions{})

	assert.NotNil(t, err)

	// It should work now as the db automatically reconnected to the leader node.
	err = db.CreatePersistentSubscription(ctx, streamID, "a_group", esdb.PersistentStreamSubscriptionOptions{})

	if err != nil {
		t.Fatalf("Failed to create persistent subscription: %v", err)
	}

	assert.Nil(t, err)
}
