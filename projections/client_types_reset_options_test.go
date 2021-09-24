package projections

import (
	"testing"

	"github.com/pivonroll/EventStore-Client-Go/protos/projections"

	"github.com/stretchr/testify/require"
)

func TestResetOptionsRequest_SetName(t *testing.T) {
	t.Run("Set once", func(t *testing.T) {
		options := ResetOptionsRequest{}
		options.SetName("name")
		require.Equal(t, "name", options.name)
	})

	t.Run("Set twice", func(t *testing.T) {
		options := ResetOptionsRequest{}
		options.SetName("name")
		options.SetName("name 2")
		require.Equal(t, "name 2", options.name)
	})
}

func TestResetOptionsRequest_SetWriteCheckpoint(t *testing.T) {
	t.Run("To false", func(t *testing.T) {
		options := ResetOptionsRequest{}
		options.SetWriteCheckpoint(false)
		require.Equal(t, false, options.writeCheckpoint)
	})

	t.Run("To false from true", func(t *testing.T) {
		options := ResetOptionsRequest{}
		options.SetWriteCheckpoint(true)
		options.SetWriteCheckpoint(false)
		require.Equal(t, false, options.writeCheckpoint)
	})

	t.Run("To true", func(t *testing.T) {
		options := ResetOptionsRequest{}
		options.SetWriteCheckpoint(true)
		require.Equal(t, true, options.writeCheckpoint)
	})

	t.Run("To true from false", func(t *testing.T) {
		options := ResetOptionsRequest{}
		options.SetWriteCheckpoint(false)
		options.SetWriteCheckpoint(true)
		require.Equal(t, true, options.writeCheckpoint)
	})
}

func TestResetOptionsRequest_Build(t *testing.T) {
	t.Run("Non empty name", func(t *testing.T) {
		options := ResetOptionsRequest{}
		options.SetName("name")
		result := options.Build()

		expectedResult := &projections.ResetReq{
			Options: &projections.ResetReq_Options{
				Name:            "name",
				WriteCheckpoint: false,
			},
		}

		require.Equal(t, expectedResult, result)
	})

	t.Run("Non empty name with trailing spaces", func(t *testing.T) {
		options := ResetOptionsRequest{}
		options.SetName(" name ")
		result := options.Build()

		expectedResult := &projections.ResetReq{
			Options: &projections.ResetReq_Options{
				Name:            " name ",
				WriteCheckpoint: false,
			},
		}

		require.Equal(t, expectedResult, result)
	})

	t.Run("WriteCheckpoint set to false", func(t *testing.T) {
		options := ResetOptionsRequest{}
		options.SetWriteCheckpoint(false)
		options.SetName("name")
		result := options.Build()

		expectedResult := &projections.ResetReq{
			Options: &projections.ResetReq_Options{
				Name:            "name",
				WriteCheckpoint: false,
			},
		}

		require.Equal(t, expectedResult, result)
	})

	t.Run("WriteCheckpoint set to true", func(t *testing.T) {
		options := ResetOptionsRequest{}
		options.SetName("name")
		options.SetWriteCheckpoint(true)
		result := options.Build()

		expectedResult := &projections.ResetReq{
			Options: &projections.ResetReq_Options{
				Name:            "name",
				WriteCheckpoint: true,
			},
		}

		require.Equal(t, expectedResult, result)
	})

	t.Run("Panics for empty name", func(t *testing.T) {
		options := ResetOptionsRequest{}
		options.SetName("")

		require.Panics(t, func() {
			options.Build()
		})
	})

	t.Run("Panics for name consisting of spaces only", func(t *testing.T) {
		options := ResetOptionsRequest{}
		options.SetName("    ")

		require.Panics(t, func() {
			options.Build()
		})
	})
}
