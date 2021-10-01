package event_streams_integration_test

import (
	"testing"

	"github.com/pivonroll/EventStore-Client-Go/event_streams"
	"github.com/pivonroll/EventStore-Client-Go/test_utils"
)

func initializeContainerAndClient(t *testing.T,
	envVariableOverrides map[string]string) (event_streams.Client, test_utils.CloseFunc) {
	grpcClient, closeFunc := test_utils.InitializeGrpcClient(t, envVariableOverrides)

	client := event_streams.ClientFactoryImpl{}.Create(grpcClient)
	return client, closeFunc
}

func initializeWithPrePopulatedDatabase(t *testing.T) (event_streams.Client, test_utils.CloseFunc) {
	grpcClient, closeFunc := test_utils.InitializeGrpcClientWithPrePopulatedDatabase(t)
	client := event_streams.ClientFactoryImpl{}.Create(grpcClient)
	return client, closeFunc
}

func initializeContainerAndClientWithCredentials(t *testing.T,
	username string,
	password string, envVariableOverrides map[string]string) (event_streams.Client, test_utils.CloseFunc) {
	grpcClient, closeFunc := test_utils.InitializeGrpcClientWithCredentials(t, username, password, envVariableOverrides)

	client := event_streams.ClientFactoryImpl{}.Create(grpcClient)
	return client, closeFunc
}
