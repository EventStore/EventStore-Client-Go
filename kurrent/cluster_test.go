package kurrent_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/v4/kurrent"
	"github.com/EventStore/EventStore-Client-Go/v4/protos/gossip"

	"github.com/stretchr/testify/assert"
)

func ClusterTests(t *testing.T) {
	t.Run("ClusterTests", func(t *testing.T) {
		t.Run("notLeaderExceptionButWorkAfterRetry", notLeaderExceptionButWorkAfterRetry)
	})
}

func ClusterRebalanceTests(t *testing.T) {
	t.Run("ClusterRebalanceTests", func(t *testing.T) {
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

		err := db.CreatePersistentSubscription(ctx, streamID, "a_group", kurrent.PersistentStreamSubscriptionOptions{})

		if esdbError, ok := kurrent.FromError(err); !ok {
			if esdbError.IsErrorCode(kurrent.ErrorCodeResourceAlreadyExists) {
				// Name generator is not random enough.
				time.Sleep(1 * time.Second)
				continue
			}
		}

		assert.NotNil(t, err)

		// It should work now as the db automatically reconnected to the leader node.
		err = db.CreatePersistentSubscription(ctx, streamID, "a_group", kurrent.PersistentStreamSubscriptionOptions{})

		if esdbErr, ok := kurrent.FromError(err); !ok {
			if esdbErr.IsErrorCode(kurrent.ErrorCodeResourceAlreadyExists) {
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
	db := CreateClient("esdb://admin:changeit@localhost:2111,localhost:2112,localhost:2113?nodepreference=leader&tlsverifycert=false", t)
	defer db.Close()

	ctx := context.Background()
	streamID := NAME_GENERATOR.Generate()

	// Start reading the stream
	options := kurrent.ReadStreamOptions{From: kurrent.Start{}}

	stream, err := db.ReadStream(ctx, streamID, options, 10)
	if err != nil {
		t.Errorf("failed to read stream: %v", err)
		return
	}

	stream.Close()

	// Simulate leader node failure
	members, err := db.Gossip(ctx)

	assert.Nil(t, err)

	for _, member := range members {
		if member.State != gossip.MemberInfo_Leader || !member.GetIsAlive() {
			continue
		}

		// Shutdown the leader node
		url := fmt.Sprintf("https://%s:%d/admin/shutdown", member.HttpEndPoint.Address, member.HttpEndPoint.Port)
		t.Log("Shutting down leader node: ", url)

		req, err := http.NewRequest("POST", url, nil)
		assert.Nil(t, err)

		req.SetBasicAuth("admin", "changeit")
		client := &http.Client{
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		}
		resp, err := client.Do(req)

		assert.Nil(t, err)
		resp.Body.Close()

		break
	}

	// Wait for the cluster to rebalance
	time.Sleep(5 * time.Second)

	// Try reading the stream again
	for count := 0; count < 10; count++ {
		stream, err = db.ReadStream(ctx, streamID, options, 10)
		if err != nil {
			continue
		}

		stream.Close()

		t.Logf("Successfully read stream after %d retries", count+1)
		return
	}

	t.Fatalf("we retried long enough but the test is still failing")
}
