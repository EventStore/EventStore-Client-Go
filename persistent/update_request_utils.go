package persistent

import (
	"fmt"

	"github.com/EventStore/EventStore-Client-Go/position"
	"github.com/EventStore/EventStore-Client-Go/protos/persistent"
	"github.com/EventStore/EventStore-Client-Go/protos/shared"
)

func updateRequestStreamProto(config SubscriptionStreamConfig) *persistent.UpdateReq {
	return &persistent.UpdateReq{
		Options: updateSubscriptionStreamConfigProto(config),
	}
}

func UpdateRequestAllOptionsProto(
	config SubscriptionUpdateAllOptionConfig,
) *persistent.UpdateReq {
	options := updateRequestAllOptionsSettingsProto(config.Position)

	return &persistent.UpdateReq{
		Options: &persistent.UpdateReq_Options{
			StreamOption: options,
			GroupName:    config.GroupName,
			Settings:     updateSubscriptionSettingsProto(config.Settings),
		},
	}
}

func updateRequestAllOptionsSettingsProto(
	pos position.Position,
) *persistent.UpdateReq_Options_All {
	options := &persistent.UpdateReq_Options_All{
		All: &persistent.UpdateReq_AllOptions{
			AllOption: nil,
		},
	}

	if pos == position.StartPosition {
		options.All.AllOption = &persistent.UpdateReq_AllOptions_Start{
			Start: &shared.Empty{},
		}
	} else if pos == position.EndPosition {
		options.All.AllOption = &persistent.UpdateReq_AllOptions_End{
			End: &shared.Empty{},
		}
	} else {
		options.All.AllOption = toUpdateRequestAllOptionsFromPosition(pos)
	}

	return options
}

func updateSubscriptionStreamConfigProto(config SubscriptionStreamConfig) *persistent.UpdateReq_Options {
	return &persistent.UpdateReq_Options{
		StreamOption: updateSubscriptionStreamSettingsProto(config.StreamOption),
		// backward compatibility
		StreamIdentifier: &shared.StreamIdentifier{
			StreamName: config.StreamOption.StreamName,
		},
		GroupName: config.GroupName,
		Settings:  updateSubscriptionSettingsProto(config.Settings),
	}
}

func updateSubscriptionStreamSettingsProto(
	streamConfig StreamSettings,
) *persistent.UpdateReq_Options_Stream {
	streamOption := &persistent.UpdateReq_Options_Stream{
		Stream: &persistent.UpdateReq_StreamOptions{
			StreamIdentifier: &shared.StreamIdentifier{
				StreamName: streamConfig.StreamName,
			},
			RevisionOption: nil,
		},
	}

	if streamConfig.Revision == Revision_Start {
		streamOption.Stream.RevisionOption = &persistent.UpdateReq_StreamOptions_Start{
			Start: &shared.Empty{},
		}
	} else if streamConfig.Revision == Revision_End {
		streamOption.Stream.RevisionOption = &persistent.UpdateReq_StreamOptions_End{
			End: &shared.Empty{},
		}
	} else {
		streamOption.Stream.RevisionOption = &persistent.UpdateReq_StreamOptions_Revision{
			Revision: uint64(streamConfig.Revision),
		}
	}

	return streamOption
}

func updateSubscriptionSettingsProto(
	settings SubscriptionSettings,
) *persistent.UpdateReq_Settings {
	return &persistent.UpdateReq_Settings{
		ResolveLinks:          settings.ResolveLinks,
		ExtraStatistics:       settings.ExtraStatistics,
		MaxRetryCount:         settings.MaxRetryCount,
		MinCheckpointCount:    settings.MinCheckpointCount,
		MaxCheckpointCount:    settings.MaxCheckpointCount,
		MaxSubscriberCount:    settings.MaxSubscriberCount,
		LiveBufferSize:        settings.LiveBufferSize,
		ReadBatchSize:         settings.ReadBatchSize,
		HistoryBufferSize:     settings.HistoryBufferSize,
		NamedConsumerStrategy: updateRequestConsumerStrategyProto(settings.NamedConsumerStrategy),
		MessageTimeout:        updateRequestMessageTimeOutInMsProto(settings.MessageTimeoutInMs),
		CheckpointAfter:       updateRequestCheckpointAfterMsProto(settings.CheckpointAfterInMs),
	}
}

func updateRequestConsumerStrategyProto(
	strategy ConsumerStrategy,
) persistent.UpdateReq_ConsumerStrategy {
	switch strategy {
	case ConsumerStrategy_DispatchToSingle:
		return persistent.UpdateReq_DispatchToSingle
	case ConsumerStrategy_Pinned:
		return persistent.UpdateReq_Pinned
	case ConsumerStrategy_RoundRobin:
		return persistent.UpdateReq_RoundRobin
	default:
		panic(fmt.Sprintf("Could not map strategy %v to proto", strategy))
	}
}

func updateRequestMessageTimeOutInMsProto(
	timeout int32,
) *persistent.UpdateReq_Settings_MessageTimeoutMs {
	return &persistent.UpdateReq_Settings_MessageTimeoutMs{
		MessageTimeoutMs: timeout,
	}
}

func updateRequestCheckpointAfterMsProto(
	checkpointAfterMs int32,
) *persistent.UpdateReq_Settings_CheckpointAfterMs {
	return &persistent.UpdateReq_Settings_CheckpointAfterMs{
		CheckpointAfterMs: checkpointAfterMs,
	}
}

// toUpdateRequestAllOptionsFromPosition ...
func toUpdateRequestAllOptionsFromPosition(position position.Position) *persistent.UpdateReq_AllOptions_Position {
	return &persistent.UpdateReq_AllOptions_Position{
		Position: &persistent.UpdateReq_Position{
			PreparePosition: position.Prepare,
			CommitPosition:  position.Commit,
		},
	}
}
