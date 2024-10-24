package esdb_test

import (
	"context"
	"fmt"
	"os/exec"
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/v4/esdb"

	"github.com/stretchr/testify/assert"
)

func ClusterTests(t *testing.T) {
	t.Run("ClusterTests", func(t *testing.T) {
		t.Run("notLeaderExceptionButWorkAfterRetry", notLeaderExceptionButWorkAfterRetry)
		t.Run("readStreamAfterClusterRebalance", readStreamAfterClusterRebalance)
	})
}

func notLeaderExceptionButWorkAfterRetry(t *testing.T) {
	// Seems on GHA, we need to try more that once because the name generator is not random enough.
	for count := 0; count < 10; count++ {
		ctx := context.Background()

		// We purposely connect to a follower node so we can trigger on not leader exception.
		db := CreateClient("esdb://admin:changeit@localhost:2111,localhost:2112,localhost:2113?nodepreference=follower&tlsverifycert=false", t)
		defer db.Close()
		streamID := NAME_GENERATOR.Generate()

		err := db.CreatePersistentSubscription(ctx, streamID, "a_group", esdb.PersistentStreamSubscriptionOptions{})

		if esdbError, ok := esdb.FromError(err); !ok {
			if esdbError.IsErrorCode(esdb.ErrorCodeResourceAlreadyExists) {
				// Name generator is not random enough.
				time.Sleep(1 * time.Second)
				continue
			}
		}

		assert.NotNil(t, err)

		// It should work now as the db automatically reconnected to the leader node.
		err = db.CreatePersistentSubscription(ctx, streamID, "a_group", esdb.PersistentStreamSubscriptionOptions{})

		if esdbErr, ok := esdb.FromError(err); !ok {
			if esdbErr.IsErrorCode(esdb.ErrorCodeResourceAlreadyExists) {
				// Freak accident considering we use random stream name, safe to assume the test was
				// successful.
				return
			}

			t.Fatalf("Failed to create persistent subscription: %v", esdbErr)
		}

		assert.Nil(t, err)
		return
	}

	t.Fatalf("we retried long enough but the test is still failing")
}

func readStreamAfterClusterRebalance(t *testing.T) {
	// We purposely connect to a leader node.
	db := CreateClient("esdb://admin:changeit@localhost:2111,localhost:2112,localhost:2113?tlsverifycert=false", t)
	defer db.Close()
	streamID := NAME_GENERATOR.Generate()

	// Start reading the stream
	options := esdb.ReadStreamOptions{From: esdb.Start{}}

	stream, err := db.ReadStream(context.Background(), streamID, options, 10)
	if err != nil {
		t.Errorf("failed to read stream: %v", err)
		return
	}

	stream.Close()

	// Simulate node failure or rebalance
	simulateClusterRebalance(t)

	// Try reading the stream again
	stream, err = db.ReadStream(context.Background(), streamID, options, 10)
	if err == nil {
		t.Errorf("unexpected to read stream after cluster rebalance: %v", err)
		stream.Close()

		return
	}

	// If we get an error, it means the client did not reconnect to the leader node.
	// Wait for the client to reconnect to the leader node.
	time.Sleep(5 * time.Second)

	// Try reading the stream again
	stream, err = db.ReadStream(context.Background(), streamID, options, 10)
	if err != nil {
		t.Errorf("failed to read stream after cluster rebalance: %v", err)
		return
	}

	stream.Close()
}

func simulateClusterRebalance(t *testing.T) {
	// Stop one of the EventStoreDB nodes to simulate failure.
	fmt.Println("Simulating cluster rebalance by stopping a node...")

	stopNodeCmd := exec.Command("docker", "stop", "esdb-node1")

	err := stopNodeCmd.Run()
	if err != nil {
		t.Fatalf("failed to stop node: %v", err)
	}
}
