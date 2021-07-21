package persistent

import (
	"testing"

	"github.com/EventStore/EventStore-Client-Go/client/filtering"
	"github.com/EventStore/EventStore-Client-Go/position"
	"github.com/EventStore/EventStore-Client-Go/protos/persistent"
	"github.com/EventStore/EventStore-Client-Go/protos/shared"
	"github.com/stretchr/testify/require"
)

func Test_createRequestProto(t *testing.T) {
	config := SubscriptionStreamConfig{
		StreamOption: StreamSettings{
			StreamName: []byte("some stream name"),
			Revision:   10,
		},
		GroupName: "some group name",
		Settings: SubscriptionSettings{
			ResolveLinks:          false,
			ExtraStatistics:       false,
			MaxRetryCount:         10,
			MinCheckpointCount:    20,
			MaxCheckpointCount:    30,
			MaxSubscriberCount:    40,
			LiveBufferSize:        50,
			ReadBatchSize:         60,
			HistoryBufferSize:     70,
			NamedConsumerStrategy: ConsumerStrategy_RoundRobin,
			MessageTimeoutInMs:    80,
			CheckpointAfterInMs:   90,
		},
	}
	expectedResult := &persistent.CreateReq{
		Options: createSubscriptionStreamConfigProto(config),
	}
	result := createRequestProto(config)
	require.Equal(t, expectedResult, result)
}

func Test_createRequestAllOptionsProto(t *testing.T) {
	config := SubscriptionAllOptionConfig{
		Position: position.Position{
			Commit:  10,
			Prepare: 20,
		},
		Filter: &filtering.SubscriptionFilterOptions{
			MaxSearchWindow:    10,
			CheckpointInterval: 20,
			SubscriptionFilter: filtering.SubscriptionFilter{
				FilterType: filtering.EventFilter,
				Prefixes:   nil,
				Regex:      "some regexp",
			},
		},
		GroupName: "group name",
		Settings: SubscriptionSettings{
			ResolveLinks:          false,
			ExtraStatistics:       false,
			MaxRetryCount:         10,
			MinCheckpointCount:    20,
			MaxCheckpointCount:    30,
			MaxSubscriberCount:    40,
			LiveBufferSize:        50,
			ReadBatchSize:         60,
			HistoryBufferSize:     70,
			NamedConsumerStrategy: ConsumerStrategy_RoundRobin,
			MessageTimeoutInMs:    80,
			CheckpointAfterInMs:   90,
		},
	}

	options, err := createRequestAllOptionsSettingsProto(config.Position, config.Filter)
	require.NoError(t, err)
	expectedResult := &persistent.CreateReq{
		Options: &persistent.CreateReq_Options{
			StreamOption: options,
			GroupName:    "group name",
			Settings:     createSubscriptionSettingsProto(config.Settings),
		},
	}

	result, err := createRequestAllOptionsProto(config)
	require.NoError(t, err)
	require.Equal(t, expectedResult, result)
}

func Test_createSubscriptionStreamConfigProto(t *testing.T) {
	config := SubscriptionStreamConfig{
		StreamOption: StreamSettings{
			StreamName: []byte("some stream name"),
			Revision:   10,
		},
		GroupName: "group name",
		Settings: SubscriptionSettings{
			ResolveLinks:          false,
			ExtraStatistics:       false,
			MaxRetryCount:         10,
			MinCheckpointCount:    20,
			MaxCheckpointCount:    30,
			MaxSubscriberCount:    40,
			LiveBufferSize:        50,
			ReadBatchSize:         60,
			HistoryBufferSize:     70,
			NamedConsumerStrategy: ConsumerStrategy_RoundRobin,
			MessageTimeoutInMs:    80,
			CheckpointAfterInMs:   90,
		},
	}
	expectedResult := &persistent.CreateReq_Options{
		StreamOption: createSubscriptionStreamSettingsProto(config.StreamOption),
		// backward compatibility
		StreamIdentifier: &shared.StreamIdentifier{
			StreamName: []byte("some stream name"),
		},
		GroupName: config.GroupName,
		Settings:  createSubscriptionSettingsProto(config.Settings),
	}
	result := createSubscriptionStreamConfigProto(config)
	require.Equal(t, expectedResult, result)
}

func Test_createSubscriptionStreamSettingsProto(t *testing.T) {
	t.Run("No start nor end revision", func(t *testing.T) {
		config := StreamSettings{
			StreamName: []byte("some name"),
			Revision:   10,
		}
		expectedResult := &persistent.CreateReq_Options_Stream{
			Stream: &persistent.CreateReq_StreamOptions{
				StreamIdentifier: &shared.StreamIdentifier{
					StreamName: []byte("some name"),
				},
				RevisionOption: &persistent.CreateReq_StreamOptions_Revision{
					Revision: uint64(10),
				},
			},
		}
		result := createSubscriptionStreamSettingsProto(config)
		require.Equal(t, expectedResult, result)
	})

	t.Run("Start revision", func(t *testing.T) {
		config := StreamSettings{
			StreamName: []byte("some name"),
			Revision:   Revision_Start,
		}
		expectedResult := &persistent.CreateReq_Options_Stream{
			Stream: &persistent.CreateReq_StreamOptions{
				StreamIdentifier: &shared.StreamIdentifier{
					StreamName: []byte("some name"),
				},
				RevisionOption: &persistent.CreateReq_StreamOptions_Start{
					Start: &shared.Empty{},
				},
			},
		}
		result := createSubscriptionStreamSettingsProto(config)
		require.Equal(t, expectedResult, result)
	})

	t.Run("End revision", func(t *testing.T) {
		config := StreamSettings{
			StreamName: []byte("some name"),
			Revision:   Revision_End,
		}
		expectedResult := &persistent.CreateReq_Options_Stream{
			Stream: &persistent.CreateReq_StreamOptions{
				StreamIdentifier: &shared.StreamIdentifier{
					StreamName: []byte("some name"),
				},
				RevisionOption: &persistent.CreateReq_StreamOptions_End{
					End: &shared.Empty{},
				},
			},
		}
		result := createSubscriptionStreamSettingsProto(config)
		require.Equal(t, expectedResult, result)
	})
}

func Test_createSubscriptionSettingsProto(t *testing.T) {
	t.Run("Resolve links", func(t *testing.T) {
		settings := SubscriptionSettings{
			ResolveLinks:          false,
			NamedConsumerStrategy: ConsumerStrategy_DispatchToSingle,
		}
		result := createSubscriptionSettingsProto(settings)
		require.Equal(t, false, result.ResolveLinks)

		settings = SubscriptionSettings{
			ResolveLinks:          true,
			NamedConsumerStrategy: ConsumerStrategy_DispatchToSingle,
		}
		result = createSubscriptionSettingsProto(settings)
		require.Equal(t, true, result.ResolveLinks)
	})

	t.Run("Extra statistics links", func(t *testing.T) {
		settings := SubscriptionSettings{
			ExtraStatistics:       false,
			NamedConsumerStrategy: ConsumerStrategy_DispatchToSingle,
		}
		result := createSubscriptionSettingsProto(settings)
		require.Equal(t, false, result.ExtraStatistics)

		settings = SubscriptionSettings{
			ExtraStatistics:       true,
			NamedConsumerStrategy: ConsumerStrategy_DispatchToSingle,
		}
		result = createSubscriptionSettingsProto(settings)
		require.Equal(t, true, result.ExtraStatistics)
	})

	t.Run("Numerical fields", func(t *testing.T) {
		settings := SubscriptionSettings{
			MaxRetryCount:         10,
			MinCheckpointCount:    20,
			MaxCheckpointCount:    30,
			MaxSubscriberCount:    40,
			LiveBufferSize:        50,
			ReadBatchSize:         60,
			HistoryBufferSize:     70,
			NamedConsumerStrategy: ConsumerStrategy_DispatchToSingle,
			MessageTimeoutInMs:    80,
			CheckpointAfterInMs:   90,
		}
		expectedResult := &persistent.CreateReq_Settings{
			MaxRetryCount:         10,
			MinCheckpointCount:    20,
			MaxCheckpointCount:    30,
			MaxSubscriberCount:    40,
			LiveBufferSize:        50,
			ReadBatchSize:         60,
			HistoryBufferSize:     70,
			NamedConsumerStrategy: consumerStrategyProto(settings.NamedConsumerStrategy),
			MessageTimeout:        messageTimeOutInMsProto(settings.MessageTimeoutInMs),
			CheckpointAfter:       checkpointAfterMsProto(settings.CheckpointAfterInMs),
		}
		result := createSubscriptionSettingsProto(settings)
		require.Equal(t, expectedResult, result)
	})
}

func Test_consumerStrategyProto(t *testing.T) {
	t.Run("Dispatch to single", func(t *testing.T) {
		result := consumerStrategyProto(ConsumerStrategy_DispatchToSingle)
		require.Equal(t, persistent.CreateReq_DispatchToSingle, result)
	})

	t.Run("Round robin", func(t *testing.T) {
		result := consumerStrategyProto(ConsumerStrategy_RoundRobin)
		require.Equal(t, persistent.CreateReq_RoundRobin, result)
	})

	t.Run("Pinned", func(t *testing.T) {
		result := consumerStrategyProto(ConsumerStrategy_Pinned)
		require.Equal(t, persistent.CreateReq_Pinned, result)
	})

	t.Run("Invalid value", func(t *testing.T) {
		require.Panics(t, func() {
			consumerStrategyProto(ConsumerStrategy(4))
		})
	})
}

func Test_messageTimeOutInMsProto(t *testing.T) {
	expectedResult := &persistent.CreateReq_Settings_MessageTimeoutMs{
		MessageTimeoutMs: 10,
	}
	result := messageTimeOutInMsProto(10)
	require.Equal(t, expectedResult, result)
}

func Test_checkpointAfterMsProto(t *testing.T) {
	expectedResult := &persistent.CreateReq_Settings_CheckpointAfterMs{
		CheckpointAfterMs: 10,
	}
	result := checkpointAfterMsProto(10)
	require.Equal(t, expectedResult, result)
}

func Test_createRequestFilterOptionsProto(t *testing.T) {
	t.Run("Event filter", func(t *testing.T) {
		config := filtering.SubscriptionFilterOptions{
			MaxSearchWindow:    10,
			CheckpointInterval: 20,
			SubscriptionFilter: filtering.SubscriptionFilter{
				FilterType: filtering.EventFilter,
				Prefixes:   nil,
				Regex:      "regexp",
			},
		}

		expectedResult := &persistent.CreateReq_AllOptions_FilterOptions{
			Filter: &persistent.CreateReq_AllOptions_FilterOptions_EventType{
				EventType: &persistent.CreateReq_AllOptions_FilterOptions_Expression{
					Prefix: nil,
					Regex:  "regexp",
				},
			},
			Window: &persistent.CreateReq_AllOptions_FilterOptions_Max{
				Max: uint32(10),
			},
			CheckpointIntervalMultiplier: uint32(20),
		}

		result, err := createRequestFilterOptionsProto(config)
		require.NoError(t, err)
		require.Equal(t, expectedResult, result)
	})

	t.Run("Event filter no prefix not regex", func(t *testing.T) {
		config := filtering.SubscriptionFilterOptions{
			MaxSearchWindow:    10,
			CheckpointInterval: 20,
			SubscriptionFilter: filtering.SubscriptionFilter{
				FilterType: filtering.EventFilter,
				Prefixes:   nil,
				Regex:      "",
			},
		}

		_, err := createRequestFilterOptionsProto(config)
		require.Equal(t, createRequestFilterOptionsProto_MustProvideRegexOrPrefixErr, err.(Error).Code())
	})

	t.Run("Event filter both prefix and regex", func(t *testing.T) {
		config := filtering.SubscriptionFilterOptions{
			MaxSearchWindow:    10,
			CheckpointInterval: 20,
			SubscriptionFilter: filtering.SubscriptionFilter{
				FilterType: filtering.EventFilter,
				Prefixes:   []string{"a", "b"},
				Regex:      "a",
			},
		}

		_, err := createRequestFilterOptionsProto(config)
		require.Equal(t, createRequestFilterOptionsProto_CanSetOnlyRegexOrPrefixErr, err.(Error).Code())
	})

	t.Run("Stream filter", func(t *testing.T) {
		config := filtering.SubscriptionFilterOptions{
			MaxSearchWindow:    10,
			CheckpointInterval: 20,
			SubscriptionFilter: filtering.SubscriptionFilter{
				FilterType: filtering.StreamFilter,
				Prefixes:   nil,
				Regex:      "regexp",
			},
		}

		expectedResult := &persistent.CreateReq_AllOptions_FilterOptions{
			Filter: &persistent.CreateReq_AllOptions_FilterOptions_StreamIdentifier{
				StreamIdentifier: &persistent.CreateReq_AllOptions_FilterOptions_Expression{
					Prefix: nil,
					Regex:  "regexp",
				},
			},
			Window: &persistent.CreateReq_AllOptions_FilterOptions_Max{
				Max: uint32(10),
			},
			CheckpointIntervalMultiplier: uint32(20),
		}

		result, err := createRequestFilterOptionsProto(config)
		require.NoError(t, err)
		require.Equal(t, expectedResult, result)
	})

	t.Run("Stream filter no prefix not regex", func(t *testing.T) {
		config := filtering.SubscriptionFilterOptions{
			MaxSearchWindow:    10,
			CheckpointInterval: 20,
			SubscriptionFilter: filtering.SubscriptionFilter{
				FilterType: filtering.StreamFilter,
				Prefixes:   nil,
				Regex:      "",
			},
		}

		_, err := createRequestFilterOptionsProto(config)
		require.Equal(t, createRequestFilterOptionsProto_MustProvideRegexOrPrefixErr, err.(Error).Code())
	})

	t.Run("Stream filter both prefix and regex", func(t *testing.T) {
		config := filtering.SubscriptionFilterOptions{
			MaxSearchWindow:    10,
			CheckpointInterval: 20,
			SubscriptionFilter: filtering.SubscriptionFilter{
				FilterType: filtering.StreamFilter,
				Prefixes:   []string{"a", "b"},
				Regex:      "a",
			},
		}

		_, err := createRequestFilterOptionsProto(config)
		require.Equal(t, createRequestFilterOptionsProto_CanSetOnlyRegexOrPrefixErr, err.(Error).Code())
	})

	t.Run("No max search window", func(t *testing.T) {
		config := filtering.SubscriptionFilterOptions{
			MaxSearchWindow:    filtering.NoMaxSearchWindow,
			CheckpointInterval: 20,
			SubscriptionFilter: filtering.SubscriptionFilter{
				FilterType: filtering.StreamFilter,
				Prefixes:   nil,
				Regex:      "regexp",
			},
		}

		expectedResult := &persistent.CreateReq_AllOptions_FilterOptions{
			Filter: &persistent.CreateReq_AllOptions_FilterOptions_StreamIdentifier{
				StreamIdentifier: &persistent.CreateReq_AllOptions_FilterOptions_Expression{
					Prefix: nil,
					Regex:  "regexp",
				},
			},
			Window: &persistent.CreateReq_AllOptions_FilterOptions_Count{
				Count: &shared.Empty{},
			},
			CheckpointIntervalMultiplier: uint32(20),
		}

		result, err := createRequestFilterOptionsProto(config)
		require.NoError(t, err)
		require.Equal(t, expectedResult, result)
	})
}

func Test_toCreateRequestAllOptionsFromPosition(t *testing.T) {
	expectedResult := &persistent.CreateReq_AllOptions_Position{
		Position: &persistent.CreateReq_Position{
			PreparePosition: 20,
			CommitPosition:  10,
		},
	}
	result := toCreateRequestAllOptionsFromPosition(position.Position{
		Commit:  10,
		Prepare: 20,
	})
	require.Equal(t, expectedResult, result)
}
