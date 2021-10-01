package projections_integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/pivonroll/EventStore-Client-Go/errors"

	"github.com/stretchr/testify/require"

	"github.com/pivonroll/EventStore-Client-Go/projections"
)

func Test_CreateContinuousProjection_TrackEmittedStreamsFalse(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t)
	defer closeFunc()

	createOptions := projections.CreateOptionsRequest{}
	createOptions.SetMode(projections.CreateConfigModeContinuousOption{
		Name:                "MyContinuous_false",
		TrackEmittedStreams: false,
	}).SetQuery("fromAll().when({$init: function (state, ev) {return {};}});")

	err := client.CreateProjection(context.Background(), createOptions)
	require.NoError(t, err)
}

func Test_CreateContinuousProjection_TrackEmittedStreamsTrue(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t)
	defer closeFunc()

	createOptions := projections.CreateOptionsRequest{}
	createOptions.SetMode(projections.CreateConfigModeContinuousOption{
		Name:                "MyContinuous_true",
		TrackEmittedStreams: true,
	}).SetQuery("fromAll().when({$init: function (state, ev) {return {};}});")

	err := client.CreateProjection(context.Background(), createOptions)
	require.NoError(t, err)
}

func Test_CreateTransientProjection(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t)
	defer closeFunc()

	createOptions := projections.CreateOptionsRequest{}
	createOptions.SetMode(projections.CreateConfigModeTransientOption{
		Name: "Transient",
	}).SetQuery("fromAll().when({$init: function (state, ev) {return {};}});")

	err := client.CreateProjection(context.Background(), createOptions)
	require.NoError(t, err)
}

func Test_CreateOneTimeProjection(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t)
	defer closeFunc()

	createOptions := projections.CreateOptionsRequest{}
	createOptions.SetMode(projections.CreateConfigModeOneTimeOption{}).
		SetQuery("fromAll().when({$init: function (state, ev) {return {};}});")

	err := client.CreateProjection(context.Background(), createOptions)
	require.NoError(t, err)
}

func Test_UpdateContinuousProjection_NoEmit(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t)
	defer closeFunc()

	createOptions := projections.CreateOptionsRequest{}
	createOptions.SetMode(projections.CreateConfigModeContinuousOption{
		Name:                "MyContinuous_no_emit",
		TrackEmittedStreams: false,
	}).SetQuery("fromAll().when({$init: function (state, ev) {return {};}});")

	err := client.CreateProjection(context.Background(), createOptions)
	require.NoError(t, err)

	updateOptions := projections.UpdateOptionsRequest{}
	updateOptions.SetName("MyContinuous_no_emit")
	updateOptions.SetQuery("fromAll().when({$init: function (s, e) {return {};}});").
		SetEmitOption(projections.UpdateOptionsEmitOptionNoEmit{})

	err = client.UpdateProjection(context.Background(), updateOptions)
	require.NoError(t, err)
}

func Test_UpdateContinuousProjection_EmitFalse(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t)
	defer closeFunc()

	createOptions := projections.CreateOptionsRequest{}
	createOptions.SetMode(projections.CreateConfigModeContinuousOption{
		Name:                "MyContinuous_emit_false",
		TrackEmittedStreams: false,
	}).SetQuery("fromAll().when({$init: function (state, ev) {return {};}});")

	err := client.CreateProjection(context.Background(), createOptions)
	require.NoError(t, err)

	updateOptions := projections.UpdateOptionsRequest{}
	updateOptions.SetName("MyContinuous_emit_false")
	updateOptions.SetQuery("fromAll().when({$init: function (s, e) {return {};}});").
		SetEmitOption(projections.UpdateOptionsEmitOptionEnabled{EmitEnabled: false})

	err = client.UpdateProjection(context.Background(), updateOptions)
	require.NoError(t, err)
}

func Test_UpdateContinuousProjection_EmitTrue(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t)
	defer closeFunc()

	createOptions := projections.CreateOptionsRequest{}
	createOptions.SetMode(projections.CreateConfigModeContinuousOption{
		Name:                "MyContinuous_emit_true",
		TrackEmittedStreams: false,
	}).SetQuery("fromAll().when({$init: function (state, ev) {return {};}});")

	err := client.CreateProjection(context.Background(), createOptions)
	require.NoError(t, err)

	updateOptions := projections.UpdateOptionsRequest{}
	updateOptions.SetName("MyContinuous_emit_true")
	updateOptions.SetQuery("fromAll().when({$init: function (s, e) {return {};}});").
		SetEmitOption(projections.UpdateOptionsEmitOptionEnabled{EmitEnabled: true})

	err = client.UpdateProjection(context.Background(), updateOptions)
	require.NoError(t, err)
}

func Test_UpdateTransientProjection_NoEmit(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t)
	defer closeFunc()

	createOptions := projections.CreateOptionsRequest{}
	createOptions.SetMode(projections.CreateConfigModeTransientOption{
		Name: "MyTransient_no_emit",
	}).SetQuery("fromAll().when({$init: function (state, ev) {return {};}});")

	err := client.CreateProjection(context.Background(), createOptions)
	require.NoError(t, err)

	updateOptions := projections.UpdateOptionsRequest{}
	updateOptions.SetName("MyTransient_no_emit")
	updateOptions.SetQuery("fromAll().when({$init: function (s, e) {return {};}});").
		SetEmitOption(projections.UpdateOptionsEmitOptionNoEmit{})

	err = client.UpdateProjection(context.Background(), updateOptions)
	require.NoError(t, err)
}

func Test_UpdateTransientProjection_EmitFalse(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t)
	defer closeFunc()

	createOptions := projections.CreateOptionsRequest{}
	createOptions.SetMode(projections.CreateConfigModeTransientOption{
		Name: "MyTransient_emit_false",
	}).SetQuery("fromAll().when({$init: function (state, ev) {return {};}});")

	err := client.CreateProjection(context.Background(), createOptions)
	require.NoError(t, err)

	updateOptions := projections.UpdateOptionsRequest{}
	updateOptions.SetName("MyTransient_emit_false")
	updateOptions.SetQuery("fromAll().when({$init: function (s, e) {return {};}});").
		SetEmitOption(projections.UpdateOptionsEmitOptionEnabled{EmitEnabled: false})

	err = client.UpdateProjection(context.Background(), updateOptions)
	require.NoError(t, err)
}

func Test_UpdateTransientProjection_EmitTrue(t *testing.T) {
	t.Skip("Only execute this test locally when only one node is running")
	client, closeFunc := initializeContainerAndClient(t)
	defer closeFunc()

	createOptions := projections.CreateOptionsRequest{}
	createOptions.SetMode(projections.CreateConfigModeTransientOption{
		Name: "MyTransient_emit_true",
	}).SetQuery("fromAll().when({$init: function (state, ev) {return {};}});")

	err := client.CreateProjection(context.Background(), createOptions)
	require.NoError(t, err)

	updateOptions := projections.UpdateOptionsRequest{}
	updateOptions.SetName("MyTransient_emit_true")
	updateOptions.SetQuery("fromAll().when({$init: function (s, e) {return {};}});").
		SetEmitOption(projections.UpdateOptionsEmitOptionEnabled{EmitEnabled: true})

	err = client.UpdateProjection(context.Background(), updateOptions)
	require.NoError(t, err)
}

func Test_AbortProjection(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t)
	defer closeFunc()

	abortOptions := projections.AbortOptionsRequest{}
	abortOptions.SetName(StandardProjectionStreams)
	err := client.AbortProjection(context.Background(), abortOptions)
	require.NoError(t, err)

	stateOptions := projections.StatisticsOptionsRequest{}
	stateOptions.SetMode(projections.StatisticsOptionsRequestModeName{
		Name: StandardProjectionStreams,
	})
	statisticsClient, err := client.GetProjectionStatistics(context.Background(), stateOptions)
	require.NoError(t, err)
	require.NotNil(t, statisticsClient)

	result, stdErr := statisticsClient.Read()
	require.NoError(t, stdErr)
	require.EqualValues(t, projections.StatisticsStatusAborted, result.Status)
}

func Test_DisableProjection(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t)
	defer closeFunc()

	disableOptions := projections.DisableOptionsRequest{}
	disableOptions.SetName(StandardProjectionStreams)
	err := client.DisableProjection(context.Background(), disableOptions)
	require.NoError(t, err)

	stateOptions := projections.StatisticsOptionsRequest{}
	stateOptions.SetMode(projections.StatisticsOptionsRequestModeName{
		Name: StandardProjectionStreams,
	})
	statisticsClient, err := client.GetProjectionStatistics(context.Background(), stateOptions)
	require.NoError(t, err)
	require.NotNil(t, statisticsClient)

	result, stdErr := statisticsClient.Read()
	require.NoError(t, stdErr)
	require.EqualValues(t, projections.StatisticsStatusStopped, result.Status)
}

func Test_EnableProjection(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t)
	defer closeFunc()

	enableOptions := projections.EnableOptionsRequest{}
	enableOptions.SetName(StandardProjectionStreams)
	err := client.EnableProjection(context.Background(), enableOptions)
	require.NoError(t, err)

	stateOptions := projections.StatisticsOptionsRequest{}
	stateOptions.SetMode(projections.StatisticsOptionsRequestModeName{
		Name: StandardProjectionStreams,
	})
	statisticsClient, err := client.GetProjectionStatistics(context.Background(), stateOptions)
	require.NoError(t, err)
	require.NotNil(t, statisticsClient)

	result, stdErr := statisticsClient.Read()
	require.NoError(t, stdErr)
	require.EqualValues(t, projections.StatisticsStatusRunning, result.Status)
}

func Test_GetResultOfProjection(t *testing.T) {
	client, eventStreamsClient, closeFunc := initializeClientAndEventStreamsClient(t)
	defer closeFunc()

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

	err := client.CreateProjection(context.Background(), createOptions)
	require.NoError(t, err)

	testEvent := testCreateEvent()
	testEvent.EventType = "count_this"
	testEvent.Data = []byte(fmt.Sprintf(eventData, 0))

	pushEventsToStream(t, eventStreamsClient, streamName, testEvent)

	time.Sleep(time.Second * 2)

	resultOptions := projections.ResultOptionsRequest{}
	resultOptions.SetName("MyContinuousProjection")
	projectionResult, err := client.
		GetProjectionResult(context.Background(), resultOptions)
	require.NoError(t, err)
	require.Equal(t, projections.ResultResponseStructType, projectionResult.GetType())

	type resultStructType struct {
		Count int64 `json:"count"`
	}

	var result resultStructType

	structResult := projectionResult.(*projections.ResultResponseStruct)

	stdErr := json.Unmarshal(structResult.Value(), &result)
	require.NoError(t, stdErr)
	require.EqualValues(t, 1, result.Count)
}

func Test_GetStateOfProjection(t *testing.T) {
	client, eventStreamsClient, closeFunc := initializeClientAndEventStreamsClient(t)
	defer closeFunc()

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

	err := client.CreateProjection(context.Background(), createOptions)
	require.NoError(t, err)

	testEvent := testCreateEvent()
	testEvent.EventType = "count_this"
	testEvent.Data = []byte(fmt.Sprintf(eventData, 0))

	pushEventsToStream(t, eventStreamsClient, streamName, testEvent)

	time.Sleep(time.Second * 2)

	resultOptions := projections.StateOptionsRequest{}
	resultOptions.SetName("MyContinuousProjection")
	projectionResult, err := client.
		GetProjectionState(context.Background(), resultOptions)
	require.NoError(t, err)
	require.Equal(t, projections.StateResponseStructType, projectionResult.GetType())

	type resultStructType struct {
		Count int64 `json:"count"`
	}

	var result resultStructType

	structResult := projectionResult.(*projections.StateResponseStruct)

	stdErr := json.Unmarshal(structResult.Value(), &result)
	require.NoError(t, stdErr)
	require.EqualValues(t, 1, result.Count)
}

func Test_GetStatusOfProjection(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t)
	defer closeFunc()

	stateOptions := projections.StatisticsOptionsRequest{}
	stateOptions.SetMode(projections.StatisticsOptionsRequestModeName{
		Name: StandardProjectionStreams,
	})
	statisticsClient, err := client.GetProjectionStatistics(context.Background(), stateOptions)
	require.NoError(t, err)
	require.NotNil(t, statisticsClient)

	result, stdErr := statisticsClient.Read()
	require.NoError(t, stdErr)
	require.EqualValues(t, StandardProjectionStreams, result.Name)
}

func Test_ResetProjection(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t)
	defer closeFunc()

	resetOptions := projections.ResetOptionsRequest{}
	resetOptions.SetName(StandardProjectionStreams)
	err := client.ResetProjection(context.Background(), resetOptions)
	require.NoError(t, err)

	stateOptions := projections.StatisticsOptionsRequest{}
	stateOptions.SetMode(projections.StatisticsOptionsRequestModeName{
		Name: StandardProjectionStreams,
	})
	statisticsClient, err := client.
		GetProjectionStatistics(context.Background(), stateOptions)
	require.NoError(t, err)
	require.NotNil(t, statisticsClient)

	result, stdErr := statisticsClient.Read()
	require.NoError(t, stdErr)
	require.EqualValues(t, projections.StatisticsStatusRunning, result.Status)
}

func Test_RestartProjectionSubsystem(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t)
	defer closeFunc()

	err := client.RestartProjectionsSubsystem(context.Background())
	require.NoError(t, err)
}

func Test_ListAllProjections(t *testing.T) {
	client, closeFunc := initializeContainerAndClient(t)
	defer closeFunc()

	expectedStreamNames := []string{
		StandardProjectionStreams,
		StandardProjectionStreamByCategory,
		StandardProjectionByCategory,
		StandardProjectionByEventType,
		StandardProjectionByCorrelationId,
	}

	result, err := client.ListAllProjections(context.Background())
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
	client, closeFunc := initializeContainerAndClient(t)
	defer closeFunc()

	createOptions := projections.CreateOptionsRequest{}
	createOptions.SetMode(projections.CreateConfigModeContinuousOption{
		Name:                "MyContinuous_false",
		TrackEmittedStreams: false,
	}).SetQuery("fromAll().when({$init: function (state, ev) {return {};}});")

	err := client.CreateProjection(context.Background(), createOptions)
	require.NoError(t, err)

	expectedStreamNames := []string{
		StandardProjectionStreams,
		StandardProjectionStreamByCategory,
		StandardProjectionByCategory,
		StandardProjectionByEventType,
		StandardProjectionByCorrelationId,
		"MyContinuous_false",
	}

	result, err := client.ListContinuousProjections(context.Background())
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
	client, closeFunc := initializeContainerAndClient(t)
	defer closeFunc()

	createOptions := projections.CreateOptionsRequest{}
	createOptions.SetMode(projections.CreateConfigModeOneTimeOption{}).
		SetQuery("fromAll().when({$init: function (state, ev) {return {};}});")

	err := client.CreateProjection(context.Background(), createOptions)
	require.NoError(t, err)

	result, err := client.ListOneTimeProjections(context.Background())
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, result, 1)

	require.Equal(t, projections.StatisticsModeOneTime, result[0].Mode)
}

func Test_CreateProjection_WithIncorrectCredentials(t *testing.T) {
	client, closeFunc := initializeContainerAndClientWithCredentials(t,
		"wrong_user_name", "wrong_password", nil)
	defer closeFunc()

	createOptions := projections.CreateOptionsRequest{}
	createOptions.SetMode(projections.CreateConfigModeOneTimeOption{}).
		SetQuery("fromAll().when({$init: function (state, ev) {return {};}});")

	err := client.CreateProjection(context.Background(), createOptions)
	require.Equal(t, errors.UnauthenticatedErr, err.Code())
}

func Test_UpdateProjection_WithIncorrectCredentials(t *testing.T) {
	client, closeFunc := initializeContainerAndClientWithCredentials(t,
		"wrong_user_name", "wrong_password", nil)
	defer closeFunc()

	updateOptions := projections.UpdateOptionsRequest{}
	updateOptions.SetName("MyTransient_no_emit")
	updateOptions.SetQuery("fromAll().when({$init: function (s, e) {return {};}});").
		SetEmitOption(projections.UpdateOptionsEmitOptionNoEmit{})

	err := client.UpdateProjection(context.Background(), updateOptions)
	require.Equal(t, errors.UnauthenticatedErr, err.Code())
}

func Test_AbortProjection_WithIncorrectCredentials(t *testing.T) {
	client, closeFunc := initializeContainerAndClientWithCredentials(t,
		"wrong_user_name", "wrong_password", nil)
	defer closeFunc()

	abortOptions := projections.AbortOptionsRequest{}
	abortOptions.SetName(StandardProjectionStreams)
	err := client.AbortProjection(context.Background(), abortOptions)
	require.Equal(t, errors.UnauthenticatedErr, err.Code())
}

func Test_DisableProjection_WithIncorrectCredentials(t *testing.T) {
	client, closeFunc := initializeContainerAndClientWithCredentials(t,
		"wrong_user_name", "wrong_password", nil)
	defer closeFunc()

	disableOptions := projections.DisableOptionsRequest{}
	disableOptions.SetName(StandardProjectionStreams)
	err := client.DisableProjection(context.Background(), disableOptions)
	require.Equal(t, errors.UnauthenticatedErr, err.Code())
}

func Test_EnableProjection_WithIncorrectCredentials(t *testing.T) {
	client, closeFunc := initializeContainerAndClientWithCredentials(t,
		"wrong_user_name", "wrong_password", nil)
	defer closeFunc()

	enableOptions := projections.EnableOptionsRequest{}
	enableOptions.SetName(StandardProjectionStreams)
	err := client.EnableProjection(context.Background(), enableOptions)
	require.Equal(t, errors.UnauthenticatedErr, err.Code())
}

func Test_GetProjectionResult_WithIncorrectCredentials(t *testing.T) {
	client, closeFunc := initializeContainerAndClientWithCredentials(t,
		"wrong_user_name", "wrong_password", nil)
	defer closeFunc()

	resultOptions := projections.ResultOptionsRequest{}
	resultOptions.SetName("MyContinuousProjection")
	_, err := client.
		GetProjectionResult(context.Background(), resultOptions)
	require.Equal(t, errors.UnauthenticatedErr, err.Code())
}

func Test_GetProjectionState_WithIncorrectCredentials(t *testing.T) {
	client, closeFunc := initializeContainerAndClientWithCredentials(t,
		"wrong_user_name", "wrong_password", nil)
	defer closeFunc()

	resultOptions := projections.StateOptionsRequest{}
	resultOptions.SetName("MyContinuousProjection")
	_, err := client.
		GetProjectionState(context.Background(), resultOptions)
	require.Equal(t, errors.UnauthenticatedErr, err.Code())
}

const (
	StandardProjectionStreams          = "$streams"
	StandardProjectionStreamByCategory = "$stream_by_category"
	StandardProjectionByCategory       = "$by_category"
	StandardProjectionByEventType      = "$by_event_type"
	StandardProjectionByCorrelationId  = "$by_correlation_id"
)
