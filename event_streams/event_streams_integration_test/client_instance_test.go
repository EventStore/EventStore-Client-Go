package event_streams_integration_test

import (
	"testing"

	"github.com/pivonroll/EventStore-Client-Go/event_streams"
	"github.com/pivonroll/EventStore-Client-Go/test_container"
)

func initializeContainerAndClient(t *testing.T,
	envVariableOverrides map[string]string) (event_streams.Client, test_container.CloseFunc) {
	grpcClient, closeFunc := test_container.InitializeContainerAndGrpcClient(t, envVariableOverrides)

	client := event_streams.ClientFactoryImpl{}.CreateClient(grpcClient)
	return client, closeFunc
}

func initializeWithPrePopulatedDatabase(t *testing.T) (event_streams.Client, test_container.CloseFunc) {
	grpcClient, closeFunc := test_container.InitializeGrpcClientWithPrePopulatedDatabase(t)
	client := event_streams.ClientFactoryImpl{}.CreateClient(grpcClient)
	return client, closeFunc
}
