package projections

import (
	"testing"

	"github.com/EventStore/EventStore-Client-Go/protos/projections"

	"github.com/stretchr/testify/require"
)

func TestResultOptionsRequest_SetName(t *testing.T) {
	t.Run("Set once", func(t *testing.T) {
		options := ResultOptionsRequest{}
		options.SetName("name")
		require.Equal(t, "name", options.name)
	})

	t.Run("Set twice", func(t *testing.T) {
		options := ResultOptionsRequest{}
		options.SetName("name")
		options.SetName("name 2")
		require.Equal(t, "name 2", options.name)
	})
}

func TestResultOptionsRequest_SetPartition(t *testing.T) {
	t.Run("Set once", func(t *testing.T) {
		options := ResultOptionsRequest{}
		options.SetPartition("partition")
		require.Equal(t, "partition", options.partition)
	})

	t.Run("Set twice", func(t *testing.T) {
		options := ResultOptionsRequest{}
		options.SetPartition("partition")
		options.SetPartition("partition 2")
		require.Equal(t, "partition 2", options.partition)
	})
}

func TestResultOptionsRequest_Build(t *testing.T) {
	t.Run("Non empty name", func(t *testing.T) {
		options := ResultOptionsRequest{}
		options.SetName("name")
		result := options.Build()

		expectedResult := &projections.ResultReq{
			Options: &projections.ResultReq_Options{
				Name: "name",
			},
		}

		require.Equal(t, expectedResult, result)
	})

	t.Run("Non empty name with trailing spaces", func(t *testing.T) {
		options := ResultOptionsRequest{}
		options.SetName(" name ")
		result := options.Build()

		expectedResult := &projections.ResultReq{
			Options: &projections.ResultReq_Options{
				Name: " name ",
			},
		}

		require.Equal(t, expectedResult, result)
	})

	t.Run("Non empty name and partition", func(t *testing.T) {
		options := ResultOptionsRequest{}
		options.SetName("name")
		options.SetPartition("partition")
		result := options.Build()

		expectedResult := &projections.ResultReq{
			Options: &projections.ResultReq_Options{
				Name:      "name",
				Partition: "partition",
			},
		}

		require.Equal(t, expectedResult, result)
	})

	t.Run("Panics for empty name", func(t *testing.T) {
		options := ResultOptionsRequest{}
		options.SetName("")

		require.Panics(t, func() {
			options.Build()
		})
	})

	t.Run("Panics for name consisting of spaces only", func(t *testing.T) {
		options := ResultOptionsRequest{}
		options.SetName("    ")

		require.Panics(t, func() {
			options.Build()
		})
	})
}
