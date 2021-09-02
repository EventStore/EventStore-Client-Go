package client_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/EventStore/EventStore-Client-Go/client"

	"github.com/stretchr/testify/require"

	"github.com/EventStore/EventStore-Client-Go/projections"
)

func initializeContainerAndClientWithProjectionsEnabled(t *testing.T) (*Container, *client.Client, CloseClientInstance) {
	return initializeContainerAndClient(t,
		"EVENTSTORE_RUN_PROJECTIONS=All",
		"EVENTSTORE_START_STANDARD_PROJECTIONS=true")
}

func Test_CreateContinuousProjection_TrackEmittedStreamsFalse(t *testing.T) {
	// instance EventStore container and client
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClientWithProjectionsEnabled(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	createOptions := projections.CreateOptionsRequest{}
	createOptions.SetMode(projections.CreateConfigModeContinuousOption{
		Name:                "MyContinuous_false",
		TrackEmittedStreams: false,
	}).SetQuery("fromAll().when({$init: function (state, ev) {return {};}});")

	err := clientInstance.CreateProjection(context.Background(), createOptions)
	require.NoError(t, err)
}

func Test_CreateContinuousProjection_TrackEmittedStreamsTrue(t *testing.T) {
	// instance EventStore container and client
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClientWithProjectionsEnabled(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	createOptions := projections.CreateOptionsRequest{}
	createOptions.SetMode(projections.CreateConfigModeContinuousOption{
		Name:                "MyContinuous_true",
		TrackEmittedStreams: true,
	}).SetQuery("fromAll().when({$init: function (state, ev) {return {};}});")

	err := clientInstance.CreateProjection(context.Background(), createOptions)
	require.NoError(t, err)
}

func Test_CreateTransientProjection(t *testing.T) {
	// instance EventStore container and client
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClientWithProjectionsEnabled(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	createOptions := projections.CreateOptionsRequest{}
	createOptions.SetMode(projections.CreateConfigModeTransientOption{
		Name: "Transient",
	}).SetQuery("fromAll().when({$init: function (state, ev) {return {};}});")

	err := clientInstance.CreateProjection(context.Background(), createOptions)
	require.NoError(t, err)
}

func Test_CreateOneTimeProjection(t *testing.T) {
	// instance EventStore container and client
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClientWithProjectionsEnabled(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	createOptions := projections.CreateOptionsRequest{}
	createOptions.SetMode(projections.CreateConfigModeOneTimeOption{}).
		SetQuery("fromAll().when({$init: function (state, ev) {return {};}});")

	err := clientInstance.CreateProjection(context.Background(), createOptions)
	require.NoError(t, err)
}

func Test_UpdateContinuousProjection_NoEmit(t *testing.T) {
	// instance EventStore container and client
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClientWithProjectionsEnabled(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	createOptions := projections.CreateOptionsRequest{}
	createOptions.SetMode(projections.CreateConfigModeContinuousOption{
		Name:                "MyContinuous_no_emit",
		TrackEmittedStreams: false,
	}).SetQuery("fromAll().when({$init: function (state, ev) {return {};}});")

	err := clientInstance.CreateProjection(context.Background(), createOptions)
	require.NoError(t, err)

	updateOptions := projections.UpdateOptionsRequest{}
	updateOptions.SetName("MyContinuous_no_emit")
	updateOptions.SetQuery("fromAll().when({$init: function (s, e) {return {};}});").
		SetEmitOption(projections.UpdateOptionsEmitOptionNoEmit{})

	err = clientInstance.UpdateProjection(context.Background(), updateOptions)
	require.NoError(t, err)
}

func Test_UpdateContinuousProjection_EmitFalse(t *testing.T) {
	// instance EventStore container and client
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClientWithProjectionsEnabled(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	createOptions := projections.CreateOptionsRequest{}
	createOptions.SetMode(projections.CreateConfigModeContinuousOption{
		Name:                "MyContinuous_emit_false",
		TrackEmittedStreams: false,
	}).SetQuery("fromAll().when({$init: function (state, ev) {return {};}});")

	err := clientInstance.CreateProjection(context.Background(), createOptions)
	require.NoError(t, err)

	updateOptions := projections.UpdateOptionsRequest{}
	updateOptions.SetName("MyContinuous_emit_false")
	updateOptions.SetQuery("fromAll().when({$init: function (s, e) {return {};}});").
		SetEmitOption(projections.UpdateOptionsEmitOptionEnabled{EmitEnabled: false})

	err = clientInstance.UpdateProjection(context.Background(), updateOptions)
	require.NoError(t, err)
}

func Test_UpdateContinuousProjection_EmitTrue(t *testing.T) {
	// instance EventStore container and client
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClientWithProjectionsEnabled(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	createOptions := projections.CreateOptionsRequest{}
	createOptions.SetMode(projections.CreateConfigModeContinuousOption{
		Name:                "MyContinuous_emit_true",
		TrackEmittedStreams: false,
	}).SetQuery("fromAll().when({$init: function (state, ev) {return {};}});")

	err := clientInstance.CreateProjection(context.Background(), createOptions)
	require.NoError(t, err)

	updateOptions := projections.UpdateOptionsRequest{}
	updateOptions.SetName("MyContinuous_emit_true")
	updateOptions.SetQuery("fromAll().when({$init: function (s, e) {return {};}});").
		SetEmitOption(projections.UpdateOptionsEmitOptionEnabled{EmitEnabled: true})

	err = clientInstance.UpdateProjection(context.Background(), updateOptions)
	require.NoError(t, err)
}

func Test_UpdateTransientProjection_NoEmit(t *testing.T) {
	// instance EventStore container and client
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClientWithProjectionsEnabled(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	createOptions := projections.CreateOptionsRequest{}
	createOptions.SetMode(projections.CreateConfigModeTransientOption{
		Name: "MyTransient_no_emit",
	}).SetQuery("fromAll().when({$init: function (state, ev) {return {};}});")

	err := clientInstance.CreateProjection(context.Background(), createOptions)
	require.NoError(t, err)

	updateOptions := projections.UpdateOptionsRequest{}
	updateOptions.SetName("MyTransient_no_emit")
	updateOptions.SetQuery("fromAll().when({$init: function (s, e) {return {};}});").
		SetEmitOption(projections.UpdateOptionsEmitOptionNoEmit{})

	err = clientInstance.UpdateProjection(context.Background(), updateOptions)
	require.NoError(t, err)
}

func Test_UpdateTransientProjection_EmitFalse(t *testing.T) {
	// instance EventStore container and client
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClientWithProjectionsEnabled(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	createOptions := projections.CreateOptionsRequest{}
	createOptions.SetMode(projections.CreateConfigModeTransientOption{
		Name: "MyTransient_emit_false",
	}).SetQuery("fromAll().when({$init: function (state, ev) {return {};}});")

	err := clientInstance.CreateProjection(context.Background(), createOptions)
	require.NoError(t, err)

	updateOptions := projections.UpdateOptionsRequest{}
	updateOptions.SetName("MyTransient_emit_false")
	updateOptions.SetQuery("fromAll().when({$init: function (s, e) {return {};}});").
		SetEmitOption(projections.UpdateOptionsEmitOptionEnabled{EmitEnabled: false})

	err = clientInstance.UpdateProjection(context.Background(), updateOptions)
	require.NoError(t, err)
}

func Test_UpdateTransientProjection_EmitTrue(t *testing.T) {
	// instance EventStore container and client
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClientWithProjectionsEnabled(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	createOptions := projections.CreateOptionsRequest{}
	createOptions.SetMode(projections.CreateConfigModeTransientOption{
		Name: "MyTransient_emit_true",
	}).SetQuery("fromAll().when({$init: function (state, ev) {return {};}});")

	err := clientInstance.CreateProjection(context.Background(), createOptions)
	require.NoError(t, err)

	updateOptions := projections.UpdateOptionsRequest{}
	updateOptions.SetName("MyTransient_emit_true")
	updateOptions.SetQuery("fromAll().when({$init: function (s, e) {return {};}});").
		SetEmitOption(projections.UpdateOptionsEmitOptionEnabled{EmitEnabled: true})

	err = clientInstance.UpdateProjection(context.Background(), updateOptions)
	require.NoError(t, err)
}

func Test_AbortProjection(t *testing.T) {
	// instance EventStore container and client
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClientWithProjectionsEnabled(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	abortOptions := projections.AbortOptionsRequest{}
	abortOptions.SetName(StandardProjectionStreams)
	err := clientInstance.AbortProjection(context.Background(), abortOptions)
	require.NoError(t, err)

	stateOptions := projections.StatisticsOptionsRequest{}
	stateOptions.SetMode(projections.StatisticsOptionsRequestModeName{
		Name: StandardProjectionStreams,
	})
	statisticsClient, err := clientInstance.GetProjectionStatistics(context.Background(), stateOptions)
	require.NoError(t, err)
	require.NotNil(t, statisticsClient)

	result, err := statisticsClient.Read()
	require.NoError(t, err)
	require.EqualValues(t, projections.StatisticsStatusAborted, result.Status)
}

func Test_DisableProjection(t *testing.T) {
	// instance EventStore container and client
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClientWithProjectionsEnabled(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	disableOptions := projections.DisableOptionsRequest{}
	disableOptions.SetName(StandardProjectionStreams)
	err := clientInstance.DisableProjection(context.Background(), disableOptions)
	require.NoError(t, err)

	stateOptions := projections.StatisticsOptionsRequest{}
	stateOptions.SetMode(projections.StatisticsOptionsRequestModeName{
		Name: StandardProjectionStreams,
	})
	statisticsClient, err := clientInstance.GetProjectionStatistics(context.Background(), stateOptions)
	require.NoError(t, err)
	require.NotNil(t, statisticsClient)

	result, err := statisticsClient.Read()
	require.NoError(t, err)
	require.EqualValues(t, projections.StatisticsStatusStopped, result.Status)
}

func Test_EnableProjection(t *testing.T) {
	// instance EventStore container and client
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClientWithProjectionsEnabled(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	enableOptions := projections.EnableOptionsRequest{}
	enableOptions.SetName(StandardProjectionStreams)
	err := clientInstance.EnableProjection(context.Background(), enableOptions)
	require.NoError(t, err)

	stateOptions := projections.StatisticsOptionsRequest{}
	stateOptions.SetMode(projections.StatisticsOptionsRequestModeName{
		Name: StandardProjectionStreams,
	})
	statisticsClient, err := clientInstance.GetProjectionStatistics(context.Background(), stateOptions)
	require.NoError(t, err)
	require.NotNil(t, statisticsClient)

	result, err := statisticsClient.Read()
	require.NoError(t, err)
	require.EqualValues(t, projections.StatisticsStatusRunning, result.Status)
}

func Test_GetResultOfProjection(t *testing.T) {
	// instance EventStore container and client
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClientWithProjectionsEnabled(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	streamName := "result_test_stream"

	const resultProjectionQuery = `
		fromStream('%s').when(
			{
				$init: function (state, ev) {
					return {
						count: 0
					};
				},
				$any: function(s, e) {
					s.count += 1;
				}
			}
		);
		`

	const eventData = `{
		message: "test",
		index: %d,
	}`

	createOptions := projections.CreateOptionsRequest{}
	createOptions.SetMode(projections.CreateConfigModeContinuousOption{
		Name:                "MyContinuousProjection",
		TrackEmittedStreams: false,
	}).SetQuery(fmt.Sprintf(resultProjectionQuery, streamName))

	err := clientInstance.CreateProjection(context.Background(), createOptions)
	require.NoError(t, err)

	testEvent := createTestEvent()
	testEvent.EventType = "count_this"
	testEvent.Data = []byte(fmt.Sprintf(eventData, 0))

	pushEventsToStream(t, clientInstance, streamName, testEvent)

	time.Sleep(time.Second * 2)

	resultOptions := projections.ResultOptionsRequest{}
	resultOptions.SetName("MyContinuousProjection")
	projectionResult, err := clientInstance.GetProjectionResult(context.Background(), resultOptions)
	require.NoError(t, err)
	require.Equal(t, projections.ResultResponseStructType, projectionResult.GetType())

	type resultStructType struct {
		Count int64 `json:"count"`
	}

	var result resultStructType

	structResult := projectionResult.(*projections.ResultResponseStruct)

	err = json.Unmarshal(structResult.Value(), &result)
	require.NoError(t, err)
	require.EqualValues(t, 1, result.Count)
}

func Test_GetStateOfProjection(t *testing.T) {
	// instance EventStore container and client
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClientWithProjectionsEnabled(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	streamName := "state_test_stream"

	const resultProjectionQuery = `
		fromStream('%s').when(
			{
				$init: function (state, ev) {
					return {
						count: 0
					};
				},
				$any: function(s, e) {
					s.count += 1;
				}
			}
		);
		`

	const eventData = `{
		message: "test",
		index: %d,
	}`

	createOptions := projections.CreateOptionsRequest{}
	createOptions.SetMode(projections.CreateConfigModeContinuousOption{
		Name:                "MyContinuousProjection",
		TrackEmittedStreams: false,
	}).SetQuery(fmt.Sprintf(resultProjectionQuery, streamName))

	err := clientInstance.CreateProjection(context.Background(), createOptions)
	require.NoError(t, err)

	testEvent := createTestEvent()
	testEvent.EventType = "count_this"
	testEvent.Data = []byte(fmt.Sprintf(eventData, 0))

	pushEventsToStream(t, clientInstance, streamName, testEvent)

	time.Sleep(time.Second * 2)

	resultOptions := projections.StateOptionsRequest{}
	resultOptions.SetName("MyContinuousProjection")
	projectionResult, err := clientInstance.GetProjectionState(context.Background(), resultOptions)
	require.NoError(t, err)
	require.Equal(t, projections.StateResponseStructType, projectionResult.GetType())

	type resultStructType struct {
		Count int64 `json:"count"`
	}

	var result resultStructType

	structResult := projectionResult.(*projections.StateResponseStruct)

	err = json.Unmarshal(structResult.Value(), &result)
	require.NoError(t, err)
	require.EqualValues(t, 1, result.Count)
}

func Test_GetStatusOfProjection(t *testing.T) {
	// instance EventStore container and client
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClientWithProjectionsEnabled(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	stateOptions := projections.StatisticsOptionsRequest{}
	stateOptions.SetMode(projections.StatisticsOptionsRequestModeName{
		Name: StandardProjectionStreams,
	})
	statisticsClient, err := clientInstance.GetProjectionStatistics(context.Background(), stateOptions)
	require.NoError(t, err)
	require.NotNil(t, statisticsClient)

	result, err := statisticsClient.Read()
	require.NoError(t, err)
	require.EqualValues(t, StandardProjectionStreams, result.Name)
}

func Test_ResetProjection(t *testing.T) {
	// instance EventStore container and client
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClientWithProjectionsEnabled(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	resetOptions := projections.ResetOptionsRequest{}
	resetOptions.SetName(StandardProjectionStreams)
	err := clientInstance.ResetProjection(context.Background(), resetOptions)
	require.NoError(t, err)

	stateOptions := projections.StatisticsOptionsRequest{}
	stateOptions.SetMode(projections.StatisticsOptionsRequestModeName{
		Name: StandardProjectionStreams,
	})
	statisticsClient, err := clientInstance.GetProjectionStatistics(context.Background(), stateOptions)
	require.NoError(t, err)
	require.NotNil(t, statisticsClient)

	result, err := statisticsClient.Read()
	require.NoError(t, err)
	require.EqualValues(t, projections.StatisticsStatusRunning, result.Status)
}

func Test_RestartProjectionSubsystem(t *testing.T) {
	// instance EventStore container and client
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClientWithProjectionsEnabled(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	err := clientInstance.RestartProjectionsSubsystem(context.Background())
	require.NoError(t, err)
}

func Test_ListAllProjections(t *testing.T) {
	// instance EventStore container and client
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClientWithProjectionsEnabled(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	expectedStreamNames := []string{
		StandardProjectionStreams,
		StandardProjectionStreamByCategory,
		StandardProjectionByCategory,
		StandardProjectionByEventType,
		StandardProjectionByCorrelationId,
	}

	result, err := clientInstance.ListAllProjections(context.Background())
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result, len(expectedStreamNames))

	var resultNames []string
	for _, resultItem := range result {
		resultNames = append(resultNames, resultItem.Name)
	}

	require.ElementsMatch(t, expectedStreamNames, resultNames)
}

func Test_ListContinuousProjections(t *testing.T) {
	// instance EventStore container and client
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClientWithProjectionsEnabled(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	createOptions := projections.CreateOptionsRequest{}
	createOptions.SetMode(projections.CreateConfigModeContinuousOption{
		Name:                "MyContinuous_false",
		TrackEmittedStreams: false,
	}).SetQuery("fromAll().when({$init: function (state, ev) {return {};}});")

	err := clientInstance.CreateProjection(context.Background(), createOptions)
	require.NoError(t, err)

	expectedStreamNames := []string{
		StandardProjectionStreams,
		StandardProjectionStreamByCategory,
		StandardProjectionByCategory,
		StandardProjectionByEventType,
		StandardProjectionByCorrelationId,
		"MyContinuous_false",
	}

	result, err := clientInstance.ListContinuousProjections(context.Background())
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result, len(expectedStreamNames))

	var resultNames []string
	for _, resultItem := range result {
		resultNames = append(resultNames, resultItem.Name)
	}

	require.ElementsMatch(t, expectedStreamNames, resultNames)
}

func Test_ListOneTimeProjections(t *testing.T) {
	// instance EventStore container and client
	containerInstance, clientInstance, closeClientInstance := initializeContainerAndClientWithProjectionsEnabled(t)
	defer closeClientInstance()
	defer containerInstance.Close()

	createOptions := projections.CreateOptionsRequest{}
	createOptions.SetMode(projections.CreateConfigModeOneTimeOption{}).
		SetQuery("fromAll().when({$init: function (state, ev) {return {};}});")

	err := clientInstance.CreateProjection(context.Background(), createOptions)
	require.NoError(t, err)

	result, err := clientInstance.ListOneTimeProjections(context.Background())
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result, 1)

	require.Equal(t, projections.StatisticsModeOneTime, result[0].Mode)
}

const (
	StandardProjectionStreams          = "$streams"
	StandardProjectionStreamByCategory = "$stream_by_category"
	StandardProjectionByCategory       = "$by_category"
	StandardProjectionByEventType      = "$by_event_type"
	StandardProjectionByCorrelationId  = "$by_correlation_id"
)
