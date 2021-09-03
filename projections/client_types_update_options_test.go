package projections

import (
	"testing"

	"github.com/EventStore/EventStore-Client-Go/protos/projections"
	"github.com/EventStore/EventStore-Client-Go/protos/shared"

	"github.com/stretchr/testify/require"
)

func TestUpdateOptionsRequest_SetQuery(t *testing.T) {
	t.Run("Set query once", func(t *testing.T) {
		options := UpdateOptionsRequest{}
		options.SetQuery("some query")
		require.Equal(t, "some query", options.query)
	})

	t.Run("Set query twice", func(t *testing.T) {
		options := UpdateOptionsRequest{}
		options.SetQuery("some query")
		options.SetQuery("some query 2")
		require.Equal(t, "some query 2", options.query)
	})
}

func TestUpdateOptionsRequest_SetName(t *testing.T) {
	t.Run("Set name once", func(t *testing.T) {
		options := UpdateOptionsRequest{}
		options.SetName("some name")
		require.Equal(t, "some name", options.name)
	})

	t.Run("Set name twice", func(t *testing.T) {
		options := UpdateOptionsRequest{}
		options.SetName("some name")
		options.SetName("some name 2")
		require.Equal(t, "some name 2", options.name)
	})
}

func TestUpdateOptionsRequest_SetEmitOption(t *testing.T) {
	t.Run("Set no emit", func(t *testing.T) {
		options := UpdateOptionsRequest{}
		options.SetEmitOption(UpdateOptionsEmitOptionNoEmit{})
		require.Equal(t, UpdateOptionsEmitOptionNoEmit{}, options.emitOption)
	})

	t.Run("Set EmitOptionEnabled with EmitEnabled set to false", func(t *testing.T) {
		options := UpdateOptionsRequest{}
		options.SetEmitOption(UpdateOptionsEmitOptionEnabled{
			EmitEnabled: false,
		})
		require.Equal(t, UpdateOptionsEmitOptionEnabled{
			EmitEnabled: false,
		}, options.emitOption)
	})

	t.Run("Set EmitOptionEnabled with EmitEnabled set to true", func(t *testing.T) {
		options := UpdateOptionsRequest{}
		options.SetEmitOption(UpdateOptionsEmitOptionEnabled{
			EmitEnabled: true,
		})
		require.Equal(t, UpdateOptionsEmitOptionEnabled{
			EmitEnabled: true,
		}, options.emitOption)
	})

	t.Run("Set no emit after EmitOptionEnabled with EmitEnabled set to false", func(t *testing.T) {
		t.Run("Set no emit", func(t *testing.T) {
			options := UpdateOptionsRequest{}
			options.SetEmitOption(UpdateOptionsEmitOptionEnabled{
				EmitEnabled: false,
			})
			options.SetEmitOption(UpdateOptionsEmitOptionNoEmit{})
			require.Equal(t, UpdateOptionsEmitOptionNoEmit{}, options.emitOption)
		})
	})

	t.Run("Set no emit after EmitOptionEnabled with EmitEnabled set to true", func(t *testing.T) {
		t.Run("Set no emit", func(t *testing.T) {
			options := UpdateOptionsRequest{}
			options.SetEmitOption(UpdateOptionsEmitOptionEnabled{
				EmitEnabled: true,
			})
			options.SetEmitOption(UpdateOptionsEmitOptionNoEmit{})
			require.Equal(t, UpdateOptionsEmitOptionNoEmit{}, options.emitOption)
		})
	})

	t.Run("Set EmitOptionEnabled with EmitEnabled set to true after EmitOptionEnabled with EmitEnabled set to false",
		func(t *testing.T) {
			t.Run("Set no emit", func(t *testing.T) {
				options := UpdateOptionsRequest{}
				options.SetEmitOption(UpdateOptionsEmitOptionEnabled{
					EmitEnabled: false,
				})
				options.SetEmitOption(UpdateOptionsEmitOptionEnabled{
					EmitEnabled: true,
				})
				require.Equal(t, UpdateOptionsEmitOptionEnabled{
					EmitEnabled: true,
				}, options.emitOption)
			})
		})

	t.Run("Set EmitOptionEnabled with EmitEnabled set to false after EmitOptionEnabled with EmitEnabled set to true",
		func(t *testing.T) {
			t.Run("Set no emit", func(t *testing.T) {
				options := UpdateOptionsRequest{}
				options.SetEmitOption(UpdateOptionsEmitOptionEnabled{
					EmitEnabled: true,
				})
				options.SetEmitOption(UpdateOptionsEmitOptionEnabled{
					EmitEnabled: false,
				})
				require.Equal(t, UpdateOptionsEmitOptionEnabled{
					EmitEnabled: false,
				}, options.emitOption)
			})
		})
}

func TestUpdateOptionsRequest_Build(t *testing.T) {
	t.Run("Build with name, query and emit option set to no emit", func(t *testing.T) {
		options := UpdateOptionsRequest{}
		options.SetQuery("some query").
			SetName("some name").
			SetEmitOption(UpdateOptionsEmitOptionNoEmit{})
		result := options.Build()

		expectedResult := &projections.UpdateReq{
			Options: &projections.UpdateReq_Options{
				Name:  "some name",
				Query: "some query",
				EmitOption: &projections.UpdateReq_Options_NoEmitOptions{
					NoEmitOptions: &shared.Empty{},
				},
			},
		}

		require.Equal(t, expectedResult, result)
	})

	t.Run("Build with name, query and emit option set to EmitOptionEnabled with EmitEnabled set to false",
		func(t *testing.T) {
			options := UpdateOptionsRequest{}
			options.SetQuery("some query").
				SetName("some name").
				SetEmitOption(UpdateOptionsEmitOptionEnabled{
					EmitEnabled: false,
				})
			result := options.Build()

			expectedResult := &projections.UpdateReq{
				Options: &projections.UpdateReq_Options{
					Name:  "some name",
					Query: "some query",
					EmitOption: &projections.UpdateReq_Options_EmitEnabled{
						EmitEnabled: false,
					},
				},
			}

			require.Equal(t, expectedResult, result)
		})

	t.Run("Build with name, query and emit option set to EmitOptionEnabled with EmitEnabled set to true",
		func(t *testing.T) {
			options := UpdateOptionsRequest{}
			options.SetQuery("some query").
				SetName("some name").
				SetEmitOption(UpdateOptionsEmitOptionEnabled{
					EmitEnabled: true,
				})
			result := options.Build()

			expectedResult := &projections.UpdateReq{
				Options: &projections.UpdateReq_Options{
					Name:  "some name",
					Query: "some query",
					EmitOption: &projections.UpdateReq_Options_EmitEnabled{
						EmitEnabled: true,
					},
				},
			}

			require.Equal(t, expectedResult, result)
		})

	t.Run("Build with name with trailing spaces", func(t *testing.T) {
		options := UpdateOptionsRequest{}
		options.SetQuery("some query").
			SetName(" some name ").
			SetEmitOption(UpdateOptionsEmitOptionNoEmit{})
		result := options.Build()

		expectedResult := &projections.UpdateReq{
			Options: &projections.UpdateReq_Options{
				Name:  " some name ",
				Query: "some query",
				EmitOption: &projections.UpdateReq_Options_NoEmitOptions{
					NoEmitOptions: &shared.Empty{},
				},
			},
		}

		require.Equal(t, expectedResult, result)
	})

	t.Run("Build with query with trailing spaces", func(t *testing.T) {
		options := UpdateOptionsRequest{}
		options.SetQuery(" some query ").
			SetName("some name").
			SetEmitOption(UpdateOptionsEmitOptionNoEmit{})
		result := options.Build()

		expectedResult := &projections.UpdateReq{
			Options: &projections.UpdateReq_Options{
				Name:  "some name",
				Query: " some query ",
				EmitOption: &projections.UpdateReq_Options_NoEmitOptions{
					NoEmitOptions: &shared.Empty{},
				},
			},
		}

		require.Equal(t, expectedResult, result)
	})

	t.Run("Build with empty name panics", func(t *testing.T) {
		options := UpdateOptionsRequest{}
		options.SetName("")

		require.Panics(t, func() {
			options.Build()
		})
	})

	t.Run("Build with name consisting of spaces panics", func(t *testing.T) {
		options := UpdateOptionsRequest{}
		options.SetName("     ")

		require.Panics(t, func() {
			options.Build()
		})
	})

	t.Run("Build with empty query panics", func(t *testing.T) {
		options := UpdateOptionsRequest{}
		options.SetQuery("")

		require.Panics(t, func() {
			options.Build()
		})
	})

	t.Run("Build with query consisting of spaces panics", func(t *testing.T) {
		options := UpdateOptionsRequest{}
		options.SetQuery("     ")

		require.Panics(t, func() {
			options.Build()
		})
	})
}
