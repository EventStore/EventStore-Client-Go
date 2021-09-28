package persistent_integration_test

import (
	"testing"

	"github.com/pivonroll/EventStore-Client-Go/event_streams"
	"github.com/pivonroll/EventStore-Client-Go/persistent"
	"github.com/pivonroll/EventStore-Client-Go/test_utils"
)

func initializeContainerAndClient(t *testing.T,
	envVariableOverrides map[string]string) (persistent.Client,
	event_streams.Client,
	test_utils.CloseFunc) {
	grpcClient, closeFunc := test_utils.InitializeGrpcClient(t, envVariableOverrides)

	client := persistent.ClientFactoryImpl{}.Create(grpcClient)

	eventStreamsClient := event_streams.ClientFactoryImpl{}.Create(grpcClient)

	return client, eventStreamsClient, closeFunc
}

func initializeWithPrePopulatedDatabase(t *testing.T) (persistent.Client, test_utils.CloseFunc) {
	grpcClient, closeFunc := test_utils.InitializeGrpcClientWithPrePopulatedDatabase(t)
	client := persistent.ClientFactoryImpl{}.Create(grpcClient)
	return client, closeFunc
}
