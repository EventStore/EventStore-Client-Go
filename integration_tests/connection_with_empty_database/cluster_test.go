package connection_integration_test

import (
	"context"
	"testing"

	"github.com/pivonroll/EventStore-Client-Go/client"
	"github.com/pivonroll/EventStore-Client-Go/connection"
	"github.com/pivonroll/EventStore-Client-Go/persistent"
	"github.com/stretchr/testify/require"
)

func Test_NotLeaderExceptionButWorkAfterRetry(t *testing.T) {
	// We purposely connect to a follower node so we can trigger on not leader exception.
	clientUri := "esdb://admin:changeit@localhost:2111,localhost:2112,localhost:2113?nodepreference=follower&tlsverifycert=false"
	config, err := client.ParseConnectionString(clientUri)
	require.NoError(t, err)
	config.GossipTimeout = 100
	grpcClient := connection.NewGrpcClient(*config)

	persistentSubscriptionClient := persistent.ClientFactoryImpl{}.Create(grpcClient)

	persistentCreateConfig := persistent.CreateOrUpdateStreamRequest{
		StreamName: "myfoobar_123456",
		GroupName:  "a_group",
		Revision: persistent.StreamRevision{
			Revision: 0,
		},
		Settings: persistent.DefaultRequestSettings,
	}

	err = persistentSubscriptionClient.CreateStreamSubscription(context.Background(),
		persistentCreateConfig)
	require.Error(t, err)

	// It should work now as the client automatically reconnected to the leader node.
	err = persistentSubscriptionClient.CreateStreamSubscription(context.Background(), persistentCreateConfig)
	require.NoError(t, err)
}
