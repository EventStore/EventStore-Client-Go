package persistent

import (
	"fmt"

	"github.com/EventStore/EventStore-Client-Go/client/filtering"
	"github.com/EventStore/EventStore-Client-Go/position"
	"github.com/EventStore/EventStore-Client-Go/protos/persistent"
	"github.com/EventStore/EventStore-Client-Go/protos/shared"
)

func CreateRequestProto(config SubscriptionStreamConfig) *persistent.CreateReq {
	return &persistent.CreateReq{
		Options: CreateSubscriptionStreamConfigProto(config),
	}
}

func CreateRequestAllOptionsProto(config SubscriptionAllOptionConfig) (*persistent.CreateReq, error) {
	options, err := createRequestAllOptionsSettingsProto(config.Position, config.Filter)
	if err != nil {
		return nil, err
	}

	return &persistent.CreateReq{
		Options: &persistent.CreateReq_Options{
			StreamOption: options,
			GroupName:    config.GroupName,
			Settings:     createSubscriptionSettingsProto(config.Settings),
		},
	}, nil
}

func createRequestAllOptionsSettingsProto(
	pos position.Position,
	filter *filtering.SubscriptionFilterOptions,
) (*persistent.CreateReq_Options_All, error) {
	options := &persistent.CreateReq_Options_All{
		All: &persistent.CreateReq_AllOptions{
			AllOption: nil,
			FilterOption: &persistent.CreateReq_AllOptions_NoFilter{
				NoFilter: &shared.Empty{},
			},
		},
	}

	if pos == position.StartPosition {
		options.All.AllOption = &persistent.CreateReq_AllOptions_Start{
			Start: &shared.Empty{},
		}
	} else if pos == position.EndPosition {
		options.All.AllOption = &persistent.CreateReq_AllOptions_End{
			End: &shared.Empty{},
		}
	} else {
		options.All.AllOption = toCreateRequestAllOptionsFromPosition(pos)
	}

	if filter != nil {
		filter, err := createRequestFilterOptionsProto(*filter)
		if err != nil {
			return nil, fmt.Errorf("failed to construct filter for all option request. Reason: %v", err)
		}
		options.All.FilterOption = &persistent.CreateReq_AllOptions_Filter{
			Filter: filter,
		}
	}

	return options, nil
}

func CreateSubscriptionStreamConfigProto(config SubscriptionStreamConfig) *persistent.CreateReq_Options {
	return &persistent.CreateReq_Options{
		StreamOption: createSubscriptionStreamSettingsProto(config.StreamOption),
		// backward compatibility
		StreamIdentifier: &shared.StreamIdentifier{
			StreamName: config.StreamOption.StreamName,
		},
		GroupName: config.GroupName,
		Settings:  createSubscriptionSettingsProto(config.Settings),
	}
}

func createSubscriptionStreamSettingsProto(
	streamConfig StreamSettings,
) *persistent.CreateReq_Options_Stream {
	streamOption := &persistent.CreateReq_Options_Stream{
		Stream: &persistent.CreateReq_StreamOptions{
			StreamIdentifier: &shared.StreamIdentifier{
				StreamName: streamConfig.StreamName,
			},
		},
	}

	if streamConfig.Revision == Revision_Start {
		streamOption.Stream.RevisionOption = &persistent.CreateReq_StreamOptions_Start{
			Start: &shared.Empty{},
		}
	} else if streamConfig.Revision == Revision_End {
		streamOption.Stream.RevisionOption = &persistent.CreateReq_StreamOptions_End{
			End: &shared.Empty{},
		}
	} else {
		streamOption.Stream.RevisionOption = &persistent.CreateReq_StreamOptions_Revision{
			Revision: uint64(streamConfig.Revision),
		}
	}

	return streamOption
}

func createSubscriptionSettingsProto(
	settings SubscriptionSettings,
) *persistent.CreateReq_Settings {
	return &persistent.CreateReq_Settings{
		ResolveLinks:          settings.ResolveLinks,
		ExtraStatistics:       settings.ExtraStatistics,
		MaxRetryCount:         settings.MaxRetryCount,
		MinCheckpointCount:    settings.MinCheckpointCount,
		MaxCheckpointCount:    settings.MaxCheckpointCount,
		MaxSubscriberCount:    settings.MaxSubscriberCount,
		LiveBufferSize:        settings.LiveBufferSize,
		ReadBatchSize:         settings.ReadBatchSize,
		HistoryBufferSize:     settings.HistoryBufferSize,
		NamedConsumerStrategy: consumerStrategyProto(settings.NamedConsumerStrategy),
		MessageTimeout:        messageTimeOutInMsProto(settings.MessageTimeoutInMs),
		CheckpointAfter:       checkpointAfterMsProto(settings.CheckpointAfterInMs),
	}
}

func consumerStrategyProto(strategy ConsumerStrategy) persistent.CreateReq_ConsumerStrategy {
	switch strategy {
	case ConsumerStrategy_DispatchToSingle:
		return persistent.CreateReq_DispatchToSingle
	case ConsumerStrategy_Pinned:
		return persistent.CreateReq_Pinned
	case ConsumerStrategy_RoundRobin:
		return persistent.CreateReq_RoundRobin
	default:
		panic(fmt.Sprintf("Could not map strategy %v to proto", strategy))
	}
}

func messageTimeOutInMsProto(timeout int32) *persistent.CreateReq_Settings_MessageTimeoutMs {
	return &persistent.CreateReq_Settings_MessageTimeoutMs{
		MessageTimeoutMs: timeout,
	}
}

func checkpointAfterMsProto(checkpointAfterMs int32) *persistent.CreateReq_Settings_CheckpointAfterMs {
	return &persistent.CreateReq_Settings_CheckpointAfterMs{
		CheckpointAfterMs: checkpointAfterMs,
	}
}

// createRequestFilterOptionsProto ...
func createRequestFilterOptionsProto(
	options filtering.SubscriptionFilterOptions,
) (*persistent.CreateReq_AllOptions_FilterOptions, error) {
	if len(options.SubscriptionFilter.Prefixes) == 0 && len(options.SubscriptionFilter.Regex) == 0 {
		return nil, fmt.Errorf("the subscription filter requires a set of prefixes or a regex")
	}
	if len(options.SubscriptionFilter.Prefixes) > 0 && len(options.SubscriptionFilter.Regex) > 0 {
		return nil, fmt.Errorf("the subscription filter may only contain a regex or a set of prefixes, but not both")
	}
	filterOptions := persistent.CreateReq_AllOptions_FilterOptions{
		CheckpointIntervalMultiplier: uint32(options.CheckpointInterval),
	}
	if options.SubscriptionFilter.FilterType == filtering.EventFilter {
		filterOptions.Filter = &persistent.CreateReq_AllOptions_FilterOptions_EventType{
			EventType: &persistent.CreateReq_AllOptions_FilterOptions_Expression{
				Prefix: options.SubscriptionFilter.Prefixes,
				Regex:  options.SubscriptionFilter.Regex,
			},
		}
	} else {
		filterOptions.Filter = &persistent.CreateReq_AllOptions_FilterOptions_StreamIdentifier{
			StreamIdentifier: &persistent.CreateReq_AllOptions_FilterOptions_Expression{
				Prefix: options.SubscriptionFilter.Prefixes,
				Regex:  options.SubscriptionFilter.Regex,
			},
		}
	}
	if options.MaxSearchWindow == filtering.NoMaxSearchWindow {
		filterOptions.Window = &persistent.CreateReq_AllOptions_FilterOptions_Count{
			Count: &shared.Empty{},
		}
	} else {
		filterOptions.Window = &persistent.CreateReq_AllOptions_FilterOptions_Max{
			Max: uint32(options.MaxSearchWindow),
		}
	}
	return &filterOptions, nil
}

// toUpdateRequestAllOptionsFromPosition ...
func toCreateRequestAllOptionsFromPosition(
	position position.Position,
) *persistent.CreateReq_AllOptions_Position {
	return &persistent.CreateReq_AllOptions_Position{
		Position: &persistent.CreateReq_Position{
			PreparePosition: position.Prepare,
			CommitPosition:  position.Commit,
		},
	}
}
