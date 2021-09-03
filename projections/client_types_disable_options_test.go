package projections

import (
	"testing"

	"github.com/EventStore/EventStore-Client-Go/protos/projections"

	"github.com/stretchr/testify/require"
)

func TestDisableOptionsRequest_SetName(t *testing.T) {
	t.Run("Set once", func(t *testing.T) {
		options := DisableOptionsRequest{}
		options.SetName("name")
		require.Equal(t, "name", options.name)
	})

	t.Run("Set twice", func(t *testing.T) {
		options := DisableOptionsRequest{}
		options.SetName("name")
		options.SetName("name 2")
		require.Equal(t, "name 2", options.name)
	})
}

func TestDisableOptionsRequest_Build(t *testing.T) {
	t.Run("Non empty name", func(t *testing.T) {
		options := DisableOptionsRequest{}
		options.SetName("name")
		result := options.Build()

		expectedResult := &projections.DisableReq{
			Options: &projections.DisableReq_Options{
				Name:            "name",
				WriteCheckpoint: false,
			},
		}

		require.Equal(t, expectedResult, result)
	})

	t.Run("Non empty name with trailing spaces", func(t *testing.T) {
		options := DisableOptionsRequest{}
		options.SetName(" name ")
		result := options.Build()

		expectedResult := &projections.DisableReq{
			Options: &projections.DisableReq_Options{
				Name:            " name ",
				WriteCheckpoint: false,
			},
		}

		require.Equal(t, expectedResult, result)
	})

	t.Run("Panics for empty name", func(t *testing.T) {
		options := DisableOptionsRequest{}
		options.SetName("")

		require.Panics(t, func() {
			options.Build()
		})
	})

	t.Run("Panics for name consisting of spaces only", func(t *testing.T) {
		options := DisableOptionsRequest{}
		options.SetName("    ")

		require.Panics(t, func() {
			options.Build()
		})
	})
}
