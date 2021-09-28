package user_management_integration_test

import (
	"testing"

	"github.com/pivonroll/EventStore-Client-Go/test_utils"
	"github.com/pivonroll/EventStore-Client-Go/user_management"
)

func initializeContainerAndClient(t *testing.T,
	envVariableOverrides map[string]string) (user_management.Client,
	test_utils.CloseFunc) {
	grpcClient, closeFunc := test_utils.InitializeContainerAndGrpcClient(t, envVariableOverrides)

	client := user_management.ClientFactoryImpl{}.Create(grpcClient)

	return client, closeFunc
}
