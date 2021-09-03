package projections

import (
	"testing"

	"github.com/EventStore/EventStore-Client-Go/protos/shared"

	"github.com/EventStore/EventStore-Client-Go/protos/projections"

	"github.com/stretchr/testify/require"
)

func TestStatisticsOptionsRequest_SetOneTimeMode(t *testing.T) {
	t.Run("Set once", func(t *testing.T) {
		options := StatisticsOptionsRequest{}
		options.SetMode(StatisticsOptionsRequestModeOneTime{})

		require.Equal(t, StatisticsOptionsRequestModeOneTime{}, options.mode)
	})

	t.Run("Set twice", func(t *testing.T) {
		options := StatisticsOptionsRequest{}
		options.SetMode(StatisticsOptionsRequestModeOneTime{})
		options.SetMode(StatisticsOptionsRequestModeOneTime{})

		require.Equal(t, StatisticsOptionsRequestModeOneTime{}, options.mode)
	})
}

func TestStatisticsOptionsRequest_SetAllMode(t *testing.T) {
	t.Run("Set once", func(t *testing.T) {
		options := StatisticsOptionsRequest{}
		options.SetMode(StatisticsOptionsRequestModeAll{})

		require.Equal(t, StatisticsOptionsRequestModeAll{}, options.mode)
	})

	t.Run("Set twice", func(t *testing.T) {
		options := StatisticsOptionsRequest{}
		options.SetMode(StatisticsOptionsRequestModeAll{})
		options.SetMode(StatisticsOptionsRequestModeAll{})

		require.Equal(t, StatisticsOptionsRequestModeAll{}, options.mode)
	})
}

func TestStatisticsOptionsRequest_SetContinuousMode(t *testing.T) {
	t.Run("Set once", func(t *testing.T) {
		options := StatisticsOptionsRequest{}
		options.SetMode(StatisticsOptionsRequestModeContinuous{})

		require.Equal(t, StatisticsOptionsRequestModeContinuous{}, options.mode)
	})

	t.Run("Set twice", func(t *testing.T) {
		options := StatisticsOptionsRequest{}
		options.SetMode(StatisticsOptionsRequestModeContinuous{})
		options.SetMode(StatisticsOptionsRequestModeContinuous{})

		require.Equal(t, StatisticsOptionsRequestModeContinuous{}, options.mode)
	})
}

func TestStatisticsOptionsRequest_SetTransientMode(t *testing.T) {
	t.Run("Set once", func(t *testing.T) {
		options := StatisticsOptionsRequest{}
		options.SetMode(StatisticsOptionsRequestModeTransient{})

		require.Equal(t, StatisticsOptionsRequestModeTransient{}, options.mode)
	})

	t.Run("Set twice", func(t *testing.T) {
		options := StatisticsOptionsRequest{}
		options.SetMode(StatisticsOptionsRequestModeTransient{})
		options.SetMode(StatisticsOptionsRequestModeTransient{})

		require.Equal(t, StatisticsOptionsRequestModeTransient{}, options.mode)
	})
}

func TestStatisticsOptionsRequest_SetNameMode(t *testing.T) {
	t.Run("Set once", func(t *testing.T) {
		options := StatisticsOptionsRequest{}
		options.SetMode(StatisticsOptionsRequestModeName{
			Name: "some name",
		})

		require.Equal(t, StatisticsOptionsRequestModeName{
			Name: "some name",
		}, options.mode)
	})

	t.Run("Set twice", func(t *testing.T) {
		options := StatisticsOptionsRequest{}
		options.SetMode(StatisticsOptionsRequestModeName{
			Name: "some name",
		})
		options.SetMode(StatisticsOptionsRequestModeName{
			Name: "some name 2",
		})

		require.Equal(t, StatisticsOptionsRequestModeName{
			Name: "some name 2",
		}, options.mode)
	})
}

func TestStatisticsOptionsRequest_Build(t *testing.T) {
	t.Run("With Mode OneTime", func(t *testing.T) {
		options := StatisticsOptionsRequest{}
		options.SetMode(StatisticsOptionsRequestModeOneTime{})
		result := options.Build()

		expectedResult := &projections.StatisticsReq{
			Options: &projections.StatisticsReq_Options{
				Mode: &projections.StatisticsReq_Options_OneTime{
					OneTime: &shared.Empty{},
				},
			},
		}

		require.Equal(t, expectedResult, result)
	})

	t.Run("With Mode All", func(t *testing.T) {
		options := StatisticsOptionsRequest{}
		options.SetMode(StatisticsOptionsRequestModeAll{})
		result := options.Build()

		expectedResult := &projections.StatisticsReq{
			Options: &projections.StatisticsReq_Options{
				Mode: &projections.StatisticsReq_Options_All{
					All: &shared.Empty{},
				},
			},
		}

		require.Equal(t, expectedResult, result)
	})

	t.Run("With Mode Continuous", func(t *testing.T) {
		options := StatisticsOptionsRequest{}
		options.SetMode(StatisticsOptionsRequestModeContinuous{})
		result := options.Build()

		expectedResult := &projections.StatisticsReq{
			Options: &projections.StatisticsReq_Options{
				Mode: &projections.StatisticsReq_Options_Continuous{
					Continuous: &shared.Empty{},
				},
			},
		}

		require.Equal(t, expectedResult, result)
	})

	t.Run("With Mode Transient", func(t *testing.T) {
		options := StatisticsOptionsRequest{}
		options.SetMode(StatisticsOptionsRequestModeTransient{})
		result := options.Build()

		expectedResult := &projections.StatisticsReq{
			Options: &projections.StatisticsReq_Options{
				Mode: &projections.StatisticsReq_Options_Transient{
					Transient: &shared.Empty{},
				},
			},
		}

		require.Equal(t, expectedResult, result)
	})

	t.Run("With Mode Name", func(t *testing.T) {
		options := StatisticsOptionsRequest{}
		options.SetMode(StatisticsOptionsRequestModeName{
			Name: "name",
		})
		result := options.Build()

		expectedResult := &projections.StatisticsReq{
			Options: &projections.StatisticsReq_Options{
				Mode: &projections.StatisticsReq_Options_Name{
					Name: "name",
				},
			},
		}

		require.Equal(t, expectedResult, result)
	})

	t.Run("With Mode Name, non empty name with trailing spaces", func(t *testing.T) {
		options := StatisticsOptionsRequest{}
		options.SetMode(StatisticsOptionsRequestModeName{
			Name: " name ",
		})
		result := options.Build()

		expectedResult := &projections.StatisticsReq{
			Options: &projections.StatisticsReq_Options{
				Mode: &projections.StatisticsReq_Options_Name{
					Name: " name ",
				},
			},
		}

		require.Equal(t, expectedResult, result)
	})

	t.Run("With Mode Name, Panics with empty name", func(t *testing.T) {
		options := StatisticsOptionsRequest{}
		options.SetMode(StatisticsOptionsRequestModeName{
			Name: "",
		})

		require.Panics(t, func() {
			options.Build()
		})
	})

	t.Run("With Mode Name, Panics with name consisting from spaces only", func(t *testing.T) {
		options := StatisticsOptionsRequest{}
		options.SetMode(StatisticsOptionsRequestModeName{
			Name: "    ",
		})

		require.Panics(t, func() {
			options.Build()
		})
	})
}
