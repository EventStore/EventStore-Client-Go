package operations_integration_test

import (
	"testing"

	"github.com/pivonroll/EventStore-Client-Go/operations"
	"github.com/pivonroll/EventStore-Client-Go/test_utils"
)

func initializeClient(t *testing.T,
	envVariableOverrides map[string]string) (operations.Client,
	test_utils.CloseFunc) {
	grpcClient, closeFunc := test_utils.InitializeGrpcClient(t, envVariableOverrides)

	client := operations.ClientFactoryImpl{}.Create(grpcClient)

	return client, closeFunc
}
