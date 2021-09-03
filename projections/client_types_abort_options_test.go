package projections

import (
	"testing"

	"github.com/EventStore/EventStore-Client-Go/protos/projections"

	"github.com/stretchr/testify/require"
)

func TestAbortOptionsRequest_SetName(t *testing.T) {
	t.Run("Set name once", func(t *testing.T) {
		options := AbortOptionsRequest{}
		options.SetName("some name")
		require.Equal(t, "some name", options.name)
	})

	t.Run("Set name twice", func(t *testing.T) {
		options := AbortOptionsRequest{}
		options.SetName("some name")
		options.SetName("some name 2")
		require.Equal(t, "some name 2", options.name)
	})
}

func TestAbortOptionsRequest_Build(t *testing.T) {
	t.Run("Non empty name", func(t *testing.T) {
		options := AbortOptionsRequest{}
		options.SetName("some name")
		result := options.Build()

		expectedResult := &projections.DisableReq{
			Options: &projections.DisableReq_Options{
				Name:            "some name",
				WriteCheckpoint: true,
			},
		}

		require.Equal(t, expectedResult, result)
	})

	t.Run("Non empty name with trailing spaces", func(t *testing.T) {
		options := AbortOptionsRequest{}
		options.SetName(" some name ")
		result := options.Build()

		expectedResult := &projections.DisableReq{
			Options: &projections.DisableReq_Options{
				Name:            " some name ",
				WriteCheckpoint: true,
			},
		}

		require.Equal(t, expectedResult, result)
	})

	t.Run("Panics for empty name", func(t *testing.T) {
		options := AbortOptionsRequest{}
		options.SetName("")
		require.Panics(t, func() {
			options.Build()
		})
	})

	t.Run("Panics for name consisting of spaces only", func(t *testing.T) {
		options := AbortOptionsRequest{}
		options.SetName("    ")
		require.Panics(t, func() {
			options.Build()
		})
	})
}
