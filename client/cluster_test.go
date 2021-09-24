package client_test

import (
	"context"
	"testing"

	"github.com/pivonroll/EventStore-Client-Go/persistent"
	"github.com/stretchr/testify/assert"
)

func Test_NotLeaderExceptionButWorkAfterRetry(t *testing.T) {
	ctx := context.Background()

	// We purposely connect to a follower node so we can trigger on not leader exception.
	client := createClientConnectedToURI("esdb://admin:changeit@localhost:2111,localhost:2112,localhost:2113?nodepreference=follower&tlsverifycert=false", t)

	config := persistent.CreateOrUpdateStreamRequest{
		StreamName: "myfoobar_123456",
		GroupName:  "a_group",
		Revision:   persistent.StreamRevision{Revision: 0},
		Settings:   persistent.DefaultRequestSettings,
	}

	err := client.PersistentSubscriptions().CreateStreamSubscription(ctx, config)

	assert.NotNil(t, err)

	// It should work now as the client automatically reconnected to the leader node.
	err = client.PersistentSubscriptions().CreateStreamSubscription(ctx, config)

	if err != nil {
		t.Fatalf("Failed to create persistent subscription: %v", err)
	}

	assert.Nil(t, err)
}
