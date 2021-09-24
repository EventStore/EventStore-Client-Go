package projections

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pivonroll/EventStore-Client-Go/connection"
	"github.com/stretchr/testify/require"
)

func TestClientFactoryImpl_CreateClient(t *testing.T) {
	ctrl := gomock.NewController(t)

	defer ctrl.Finish()

	grpcClient := connection.NewMockGrpcClient(ctrl)

	factory := ClientFactoryImpl{}
	result := factory.CreateClient(grpcClient)

	expectedResult := newClientImpl(grpcClient, grpcProjectionsClientFactoryImpl{})

	require.Equal(t, expectedResult, result)
}
