package projections

import (
	"testing"

	"github.com/EventStore/EventStore-Client-Go/protos/projections"
	"github.com/EventStore/EventStore-Client-Go/protos/shared"

	"github.com/stretchr/testify/require"
)

func TestCreateOptionsRequest_SetMode(t *testing.T) {
	t.Run("Set OneTime Mode once", func(t *testing.T) {
		options := CreateOptionsRequest{}
		options.SetMode(CreateConfigModeOneTimeOption{})

		require.Equal(t, CreateConfigModeOneTimeOption{}, options.mode)
	})

	t.Run("Set OneTimeOption Mode twice", func(t *testing.T) {
		options := CreateOptionsRequest{}
		options.SetMode(CreateConfigModeOneTimeOption{})
		options.SetMode(CreateConfigModeOneTimeOption{})

		require.Equal(t, CreateConfigModeOneTimeOption{}, options.mode)
	})

	t.Run("Set OneTimeOption Mode after Continuous Mode", func(t *testing.T) {
		options := CreateOptionsRequest{}
		options.SetMode(CreateConfigModeContinuousOption{})
		options.SetMode(CreateConfigModeOneTimeOption{})

		require.Equal(t, CreateConfigModeOneTimeOption{}, options.mode)
	})

	t.Run("Set OneTimeOption Mode after Transient Mode", func(t *testing.T) {
		options := CreateOptionsRequest{}
		options.SetMode(CreateConfigModeTransientOption{})
		options.SetMode(CreateConfigModeOneTimeOption{})

		require.Equal(t, CreateConfigModeOneTimeOption{}, options.mode)
	})

	t.Run("Set Continuous Mode once", func(t *testing.T) {
		options := CreateOptionsRequest{}
		options.SetMode(CreateConfigModeContinuousOption{})

		require.Equal(t, CreateConfigModeContinuousOption{}, options.mode)
	})

	t.Run("Set Continuous Mode twice", func(t *testing.T) {
		options := CreateOptionsRequest{}
		options.SetMode(CreateConfigModeContinuousOption{})
		options.SetMode(CreateConfigModeContinuousOption{})

		require.Equal(t, CreateConfigModeContinuousOption{}, options.mode)
	})

	t.Run("Set Transient Mode once", func(t *testing.T) {
		options := CreateOptionsRequest{}
		options.SetMode(CreateConfigModeTransientOption{})

		require.Equal(t, CreateConfigModeTransientOption{}, options.mode)
	})

	t.Run("Set Continuous Mode twice", func(t *testing.T) {
		options := CreateOptionsRequest{}
		options.SetMode(CreateConfigModeTransientOption{})
		options.SetMode(CreateConfigModeTransientOption{})

		require.Equal(t, CreateConfigModeTransientOption{}, options.mode)
	})
}

func TestCreateOptionsRequest_SetQuery(t *testing.T) {
	t.Run("Set query once", func(t *testing.T) {
		options := CreateOptionsRequest{}
		options.SetQuery("some query")

		require.Equal(t, "some query", options.query)
	})

	t.Run("Set query twice", func(t *testing.T) {
		options := CreateOptionsRequest{}
		options.SetQuery("some query")
		options.SetQuery("some query 2")

		require.Equal(t, "some query 2", options.query)
	})
}

func TestCreateOptionsRequest_Build(t *testing.T) {
	t.Run("Build once with query and mode", func(t *testing.T) {
		options := CreateOptionsRequest{}
		options.SetQuery("some query")
		options.SetMode(CreateConfigModeOneTimeOption{})

		result := options.Build()

		expectedResult := &projections.CreateReq{
			Options: &projections.CreateReq_Options{
				Mode: &projections.CreateReq_Options_OneTime{
					OneTime: &shared.Empty{},
				},
				Query: "some query",
			},
		}

		require.Equal(t, expectedResult, result)
	})

	t.Run("Build once query with trailing space is not transformed", func(t *testing.T) {
		options := CreateOptionsRequest{}
		options.SetQuery(" some query ")
		options.SetMode(CreateConfigModeOneTimeOption{})

		result := options.Build()

		expectedResult := &projections.CreateReq{
			Options: &projections.CreateReq_Options{
				Mode: &projections.CreateReq_Options_OneTime{
					OneTime: &shared.Empty{},
				},
				Query: " some query ",
			},
		}

		require.Equal(t, expectedResult, result)
	})

	t.Run("Build once with empty query panics", func(t *testing.T) {
		options := CreateOptionsRequest{}
		options.SetMode(CreateConfigModeOneTimeOption{})

		require.Panics(t, func() {
			options.Build()
		})
	})

	t.Run("Build once with query consisting of spaces panics", func(t *testing.T) {
		options := CreateOptionsRequest{}
		options.SetMode(CreateConfigModeOneTimeOption{})
		options.SetQuery("    ")

		require.Panics(t, func() {
			options.Build()
		})
	})

	t.Run("Build once without mode panics", func(t *testing.T) {
		options := CreateOptionsRequest{}
		options.SetQuery("some query")

		require.Panics(t, func() {
			options.Build()
		})
	})
}
