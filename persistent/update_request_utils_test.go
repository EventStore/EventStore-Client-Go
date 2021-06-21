package persistent

import (
	"testing"

	"github.com/EventStore/EventStore-Client-Go/position"
	"github.com/EventStore/EventStore-Client-Go/protos/persistent"
	"github.com/EventStore/EventStore-Client-Go/protos/shared"
	"github.com/stretchr/testify/require"
)

func Test_updateRequestStreamProto(t *testing.T) {
	config := SubscriptionStreamConfig{
		StreamOption: StreamSettings{
			StreamName: []byte("stream name"),
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

	expectedResult := &persistent.UpdateReq{
		Options: updateSubscriptionStreamConfigProto(config),
	}

	result := updateRequestStreamProto(config)
	require.Equal(t, expectedResult, result)
}

func Test_updateSubscriptionStreamConfigProto(t *testing.T) {
	config := SubscriptionStreamConfig{
		StreamOption: StreamSettings{
			StreamName: []byte("stream name"),
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

	expectedResult := &persistent.UpdateReq_Options{
		StreamOption: updateSubscriptionStreamSettingsProto(config.StreamOption),
		// backward compatibility
		StreamIdentifier: &shared.StreamIdentifier{
			StreamName: []byte("stream name"),
		},
		GroupName: "some group name",
		Settings:  updateSubscriptionSettingsProto(config.Settings),
	}
	result := updateSubscriptionStreamConfigProto(config)
	require.Equal(t, expectedResult, result)
}

func Test_updateSubscriptionStreamSettingsProto_NonStartOrEndRevision(t *testing.T) {
	t.Run("No start nor end revision", func(t *testing.T) {
		streamConfig := StreamSettings{
			StreamName: []byte("stream name"),
			Revision:   10,
		}

		expectedResult := &persistent.UpdateReq_Options_Stream{
			Stream: &persistent.UpdateReq_StreamOptions{
				StreamIdentifier: &shared.StreamIdentifier{
					StreamName: []byte("stream name"),
				},
				RevisionOption: &persistent.UpdateReq_StreamOptions_Revision{
					Revision: uint64(10),
				},
			},
		}

		result := updateSubscriptionStreamSettingsProto(streamConfig)
		require.Equal(t, expectedResult, result)
	})

	t.Run("Start revision", func(t *testing.T) {
		streamConfig := StreamSettings{
			StreamName: []byte("stream name"),
			Revision:   Revision_Start,
		}

		expectedResult := &persistent.UpdateReq_Options_Stream{
			Stream: &persistent.UpdateReq_StreamOptions{
				StreamIdentifier: &shared.StreamIdentifier{
					StreamName: []byte("stream name"),
				},
				RevisionOption: &persistent.UpdateReq_StreamOptions_Start{
					Start: &shared.Empty{},
				},
			},
		}

		result := updateSubscriptionStreamSettingsProto(streamConfig)
		require.Equal(t, expectedResult, result)
	})

	t.Run("End revision", func(t *testing.T) {
		streamConfig := StreamSettings{
			StreamName: []byte("stream name"),
			Revision:   Revision_End,
		}

		expectedResult := &persistent.UpdateReq_Options_Stream{
			Stream: &persistent.UpdateReq_StreamOptions{
				StreamIdentifier: &shared.StreamIdentifier{
					StreamName: []byte("stream name"),
				},
				RevisionOption: &persistent.UpdateReq_StreamOptions_End{
					End: &shared.Empty{},
				},
			},
		}

		result := updateSubscriptionStreamSettingsProto(streamConfig)
		require.Equal(t, expectedResult, result)
	})
}

func Test_updateSubscriptionStreamSettingsProto_StartRevision(t *testing.T) {
}

func Test_updateSubscriptionSettingsProto(t *testing.T) {
	settings := SubscriptionSettings{
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
	}

	expectedResult := &persistent.UpdateReq_Settings{
		ResolveLinks:          false,
		ExtraStatistics:       false,
		MaxRetryCount:         10,
		MinCheckpointCount:    20,
		MaxCheckpointCount:    30,
		MaxSubscriberCount:    40,
		LiveBufferSize:        50,
		ReadBatchSize:         60,
		HistoryBufferSize:     70,
		NamedConsumerStrategy: updateRequestConsumerStrategyProto(settings.NamedConsumerStrategy),
		MessageTimeout:        updateRequestMessageTimeOutInMsProto(settings.MessageTimeoutInMs),
		CheckpointAfter:       updateRequestCheckpointAfterMsProto(settings.CheckpointAfterInMs),
	}

	result := updateSubscriptionSettingsProto(settings)
	require.Equal(t, expectedResult, result)
}

func Test_updateRequestConsumerStrategyProto(t *testing.T) {
	t.Run("Round robin", func(t *testing.T) {
		result := updateRequestConsumerStrategyProto(ConsumerStrategy_RoundRobin)
		require.Equal(t, persistent.UpdateReq_RoundRobin, result)
	})

	t.Run("Dispatch to single", func(t *testing.T) {
		result := updateRequestConsumerStrategyProto(ConsumerStrategy_DispatchToSingle)
		require.Equal(t, persistent.UpdateReq_DispatchToSingle, result)
	})

	t.Run("Pinned", func(t *testing.T) {
		result := updateRequestConsumerStrategyProto(ConsumerStrategy_Pinned)
		require.Equal(t, persistent.UpdateReq_Pinned, result)
	})

	t.Run("Invalid value", func(t *testing.T) {
		require.Panics(t, func() {
			updateRequestConsumerStrategyProto(ConsumerStrategy(4))
		})
	})
}

func Test_updateRequestMessageTimeOutInMsProto(t *testing.T) {
	expectedResult := &persistent.UpdateReq_Settings_MessageTimeoutMs{
		MessageTimeoutMs: 10,
	}
	result := updateRequestMessageTimeOutInMsProto(10)
	require.Equal(t, expectedResult, result)
}

func Test_updateRequestCheckpointAfterMsProto(t *testing.T) {
	expectedResult := &persistent.UpdateReq_Settings_CheckpointAfterMs{
		CheckpointAfterMs: 10,
	}
	result := updateRequestCheckpointAfterMsProto(10)
	require.Equal(t, expectedResult, result)
}

func Test_toUpdateRequestAllOptionsFromPosition(t *testing.T) {
	expectedResult := &persistent.UpdateReq_AllOptions_Position{
		Position: &persistent.UpdateReq_Position{
			PreparePosition: 20,
			CommitPosition:  10,
		},
	}
	result := toUpdateRequestAllOptionsFromPosition(position.Position{
		Commit:  10,
		Prepare: 20,
	})
	require.Equal(t, expectedResult, result)
}
