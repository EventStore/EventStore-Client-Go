package client_test

import (
	"context"
	"testing"

	"github.com/EventStore/EventStore-Client-Go/persistent"
	"github.com/stretchr/testify/assert"
)

func Test_NotLeaderExceptionButWorkAfterRetry(t *testing.T) {
	ctx := context.Background()

	// We purposely connect to a follower node so we can trigger on not leader exception.
	client := CreateClient("esdb://admin:changeit@localhost:2111,localhost:2112,localhost:2113?nodepreference=follower&tlsverifycert=false", t)
	config := persistent.SubscriptionStreamConfig{
		StreamOption: persistent.StreamSettings{
			StreamName: []byte("myfoobar_123456"),
			Revision:   0,
		},
		GroupName: "a_group",
		Settings:  persistent.DefaultSubscriptionSettings,
	}

	err := client.CreatePersistentSubscription(ctx, config)

	assert.NotNil(t, err)

	// It should work now as the client automatically reconnected to the leader node.
	err = client.CreatePersistentSubscription(ctx, config)

	if err != nil {
		t.Fatalf("Failed to create persistent subscription: %v", err)
	}

	assert.Nil(t, err)
}
