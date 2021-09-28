package projections_integration_test

import (
	"testing"

	"github.com/pivonroll/EventStore-Client-Go/event_streams"
	"github.com/pivonroll/EventStore-Client-Go/projections"
	"github.com/pivonroll/EventStore-Client-Go/test_container"
)

func initializeContainerAndClient(t *testing.T) (projections.Client,
	test_container.CloseFunc) {
	grpcClient, closeFunc := test_container.InitializeContainerAndGrpcClient(t,
		map[string]string{
			"EVENTSTORE_RUN_PROJECTIONS":            "All",
			"EVENTSTORE_START_STANDARD_PROJECTIONS": "true",
		})

	client := projections.ClientFactoryImpl{}.CreateClient(grpcClient)

	return client, closeFunc
}

func initializeClientAndEventStreamsClient(t *testing.T) (projections.Client,
	event_streams.Client,
	test_container.CloseFunc) {
	grpcClient, closeFunc := test_container.InitializeContainerAndGrpcClient(t,
		map[string]string{
			"EVENTSTORE_RUN_PROJECTIONS":            "All",
			"EVENTSTORE_START_STANDARD_PROJECTIONS": "true",
		})

	client := projections.ClientFactoryImpl{}.CreateClient(grpcClient)
	eventStreamsClient := event_streams.ClientFactoryImpl{}.CreateClient(grpcClient)

	return client, eventStreamsClient, closeFunc
}
