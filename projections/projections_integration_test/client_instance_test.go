package projections_integration_test

import (
	"testing"

	"github.com/pivonroll/EventStore-Client-Go/event_streams"
	"github.com/pivonroll/EventStore-Client-Go/projections"
	"github.com/pivonroll/EventStore-Client-Go/test_utils"
)

func initializeContainerAndClient(t *testing.T) (projections.Client,
	test_utils.CloseFunc) {
	grpcClient, closeFunc := test_utils.InitializeContainerAndGrpcClient(t,
		map[string]string{
			"EVENTSTORE_RUN_PROJECTIONS":            "All",
			"EVENTSTORE_START_STANDARD_PROJECTIONS": "true",
		})

	client := projections.ClientFactoryImpl{}.Create(grpcClient)

	return client, closeFunc
}

func initializeClientAndEventStreamsClient(t *testing.T) (projections.Client,
	event_streams.Client,
	test_utils.CloseFunc) {
	grpcClient, closeFunc := test_utils.InitializeContainerAndGrpcClient(t,
		map[string]string{
			"EVENTSTORE_RUN_PROJECTIONS":            "All",
			"EVENTSTORE_START_STANDARD_PROJECTIONS": "true",
		})

	client := projections.ClientFactoryImpl{}.Create(grpcClient)
	eventStreamsClient := event_streams.ClientFactoryImpl{}.Create(grpcClient)

	return client, eventStreamsClient, closeFunc
}
