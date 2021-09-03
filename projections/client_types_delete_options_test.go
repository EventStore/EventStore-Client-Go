package projections

import (
	"testing"

	"github.com/EventStore/EventStore-Client-Go/protos/projections"

	"github.com/stretchr/testify/require"
)

func TestDeleteOptionsRequest_SetDeleteEmittedStreams(t *testing.T) {
	t.Run("To false", func(t *testing.T) {
		options := DeleteOptionsRequest{}
		options.SetDeleteEmittedStreams(false)
		require.Equal(t, false, options.deleteEmittedStreams)
	})

	t.Run("To false from true", func(t *testing.T) {
		options := DeleteOptionsRequest{}
		options.SetDeleteEmittedStreams(true)
		options.SetDeleteEmittedStreams(false)
		require.Equal(t, false, options.deleteEmittedStreams)
	})

	t.Run("To true", func(t *testing.T) {
		options := DeleteOptionsRequest{}
		options.SetDeleteEmittedStreams(true)
		require.Equal(t, true, options.deleteEmittedStreams)
	})

	t.Run("To true from false", func(t *testing.T) {
		options := DeleteOptionsRequest{}
		options.SetDeleteEmittedStreams(false)
		options.SetDeleteEmittedStreams(true)
		require.Equal(t, true, options.deleteEmittedStreams)
	})
}

func TestDeleteOptionsRequest_SetDeleteStateStream(t *testing.T) {
	t.Run("To false", func(t *testing.T) {
		options := DeleteOptionsRequest{}
		options.SetDeleteStateStream(false)
		require.Equal(t, false, options.deleteStateStream)
	})

	t.Run("To false from true", func(t *testing.T) {
		options := DeleteOptionsRequest{}
		options.SetDeleteStateStream(true)
		options.SetDeleteStateStream(false)
		require.Equal(t, false, options.deleteStateStream)
	})

	t.Run("To true", func(t *testing.T) {
		options := DeleteOptionsRequest{}
		options.SetDeleteStateStream(true)
		require.Equal(t, true, options.deleteStateStream)
	})

	t.Run("To true from false", func(t *testing.T) {
		options := DeleteOptionsRequest{}
		options.SetDeleteStateStream(false)
		options.SetDeleteStateStream(true)
		require.Equal(t, true, options.deleteStateStream)
	})
}

func TestDeleteOptionsRequest_SetDeleteCheckpointStream(t *testing.T) {
	t.Run("To false", func(t *testing.T) {
		options := DeleteOptionsRequest{}
		options.SetDeleteCheckpointStream(false)
		require.Equal(t, false, options.deleteCheckpointStream)
	})

	t.Run("To false from true", func(t *testing.T) {
		options := DeleteOptionsRequest{}
		options.SetDeleteCheckpointStream(true)
		options.SetDeleteCheckpointStream(false)
		require.Equal(t, false, options.deleteCheckpointStream)
	})

	t.Run("To true", func(t *testing.T) {
		options := DeleteOptionsRequest{}
		options.SetDeleteCheckpointStream(true)
		require.Equal(t, true, options.deleteCheckpointStream)
	})

	t.Run("To true from false", func(t *testing.T) {
		options := DeleteOptionsRequest{}
		options.SetDeleteCheckpointStream(false)
		options.SetDeleteCheckpointStream(true)
		require.Equal(t, true, options.deleteCheckpointStream)
	})
}

func TestDeleteOptionsRequest_SetName(t *testing.T) {
	t.Run("Set once", func(t *testing.T) {
		options := DeleteOptionsRequest{}
		options.SetName("name")
		require.Equal(t, "name", options.name)
	})

	t.Run("Set twice", func(t *testing.T) {
		options := DeleteOptionsRequest{}
		options.SetName("name")
		options.SetName("name 2")
		require.Equal(t, "name 2", options.name)
	})
}

func TestDeleteOptionsRequest_Build(t *testing.T) {
	t.Run("Set non empty name without trailing spaces", func(t *testing.T) {
		options := DeleteOptionsRequest{}
		options.SetName("name").
			SetDeleteStateStream(false).
			SetDeleteCheckpointStream(false).
			SetDeleteEmittedStreams(false)

		result := options.Build()

		expectedResult := &projections.DeleteReq{
			Options: &projections.DeleteReq_Options{
				Name:                   "name",
				DeleteEmittedStreams:   false,
				DeleteStateStream:      false,
				DeleteCheckpointStream: false,
			},
		}

		require.Equal(t, expectedResult, result)
	})

	t.Run("Set non empty name with trailing spaces", func(t *testing.T) {
		options := DeleteOptionsRequest{}
		options.SetName(" name ").
			SetDeleteStateStream(false).
			SetDeleteCheckpointStream(false).
			SetDeleteEmittedStreams(false)

		result := options.Build()

		expectedResult := &projections.DeleteReq{
			Options: &projections.DeleteReq_Options{
				Name:                   " name ",
				DeleteEmittedStreams:   false,
				DeleteStateStream:      false,
				DeleteCheckpointStream: false,
			},
		}

		require.Equal(t, expectedResult, result)
	})

	t.Run("Set non empty name and DeleteEmittedStreams true", func(t *testing.T) {
		options := DeleteOptionsRequest{}
		options.SetName("name").
			SetDeleteEmittedStreams(true).
			SetDeleteStateStream(false).
			SetDeleteCheckpointStream(false)

		result := options.Build()

		expectedResult := &projections.DeleteReq{
			Options: &projections.DeleteReq_Options{
				Name:                   "name",
				DeleteEmittedStreams:   true,
				DeleteStateStream:      false,
				DeleteCheckpointStream: false,
			},
		}

		require.Equal(t, expectedResult, result)
	})

	t.Run("Set non empty name and SetDeleteStateStream true", func(t *testing.T) {
		options := DeleteOptionsRequest{}
		options.SetName("name").
			SetDeleteEmittedStreams(false).
			SetDeleteStateStream(true).
			SetDeleteCheckpointStream(false)

		result := options.Build()

		expectedResult := &projections.DeleteReq{
			Options: &projections.DeleteReq_Options{
				Name:                   "name",
				DeleteEmittedStreams:   false,
				DeleteStateStream:      true,
				DeleteCheckpointStream: false,
			},
		}

		require.Equal(t, expectedResult, result)
	})

	t.Run("Set non empty name and SetDeleteStateStream true", func(t *testing.T) {
		options := DeleteOptionsRequest{}
		options.SetName("name").
			SetDeleteEmittedStreams(false).
			SetDeleteStateStream(false).
			SetDeleteCheckpointStream(true)

		result := options.Build()

		expectedResult := &projections.DeleteReq{
			Options: &projections.DeleteReq_Options{
				Name:                   "name",
				DeleteEmittedStreams:   false,
				DeleteStateStream:      false,
				DeleteCheckpointStream: true,
			},
		}

		require.Equal(t, expectedResult, result)
	})

	t.Run("Panics for empty name", func(t *testing.T) {
		options := DeleteOptionsRequest{}
		options.SetName("")
		require.Panics(t, func() {
			options.Build()
		})
	})

	t.Run("Panics for name consisting of spaces only", func(t *testing.T) {
		options := DeleteOptionsRequest{}
		options.SetName("     ")
		require.Panics(t, func() {
			options.Build()
		})
	})
}
