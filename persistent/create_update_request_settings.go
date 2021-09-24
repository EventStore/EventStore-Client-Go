package persistent

import (
	"fmt"

	"github.com/pivonroll/EventStore-Client-Go/protos/persistent"
)

const SUBSCRIBER_COUNT_UNLIMITED = 0

type ConsumerStrategy int32

const (
	ConsumerStrategy_RoundRobin       ConsumerStrategy = 0
	ConsumerStrategy_DispatchToSingle ConsumerStrategy = 1
	ConsumerStrategy_Pinned           ConsumerStrategy = 2
)

type CreateOrUpdateRequestSettings struct {
	ResolveLinks          bool
	ExtraStatistics       bool
	MaxRetryCount         int32
	MinCheckpointCount    int32
	MaxCheckpointCount    int32
	MaxSubscriberCount    int32
	LiveBufferSize        int32
	ReadBatchSize         int32
	HistoryBufferSize     int32
	NamedConsumerStrategy ConsumerStrategy
	// MessageTimeoutInMs
	// MessageTimeoutInTicks
	MessageTimeout isCreateRequestMessageTimeout
	// CheckpointAfterMs
	// CheckpointAfterTicks
	CheckpointAfter isCreateRequestCheckpointAfter
}

func (settings CreateOrUpdateRequestSettings) buildCreateRequestSettings() *persistent.CreateReq_Settings {
	result := &persistent.CreateReq_Settings{
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
	}

	settings.MessageTimeout.buildCreateRequestSettings(result)
	settings.CheckpointAfter.buildCreateRequestSettings(result)

	return result
}

func (settings CreateOrUpdateRequestSettings) buildUpdateRequestSettings() *persistent.UpdateReq_Settings {
	result := &persistent.UpdateReq_Settings{
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
	}

	settings.MessageTimeout.buildUpdateRequestSettings(result)
	settings.CheckpointAfter.buildUpdateRequestSettings(result)

	return result
}

var DefaultRequestSettings = CreateOrUpdateRequestSettings{
	ResolveLinks:          false,
	ExtraStatistics:       false,
	MaxRetryCount:         10,
	MinCheckpointCount:    10,
	MaxCheckpointCount:    10 * 1000,
	MaxSubscriberCount:    SUBSCRIBER_COUNT_UNLIMITED,
	LiveBufferSize:        500,
	ReadBatchSize:         20,
	HistoryBufferSize:     500,
	NamedConsumerStrategy: ConsumerStrategy_RoundRobin,
	MessageTimeout: MessageTimeoutInMs{
		MilliSeconds: 30_000,
	},
	CheckpointAfter: CheckpointAfterMs{
		MilliSeconds: 2_000,
	},
}

type isCreateRequestMessageTimeout interface {
	isCreateRequestMessageTimeout()
	buildCreateRequestSettings(*persistent.CreateReq_Settings)
	buildUpdateRequestSettings(*persistent.UpdateReq_Settings)
}

type MessageTimeoutInMs struct {
	MilliSeconds int32
}

func (c MessageTimeoutInMs) isCreateRequestMessageTimeout() {
}

func (c MessageTimeoutInMs) buildCreateRequestSettings(
	protoSettings *persistent.CreateReq_Settings) {
	protoSettings.MessageTimeout = &persistent.CreateReq_Settings_MessageTimeoutMs{
		MessageTimeoutMs: c.MilliSeconds,
	}
}

func (c MessageTimeoutInMs) buildUpdateRequestSettings(
	protoSettings *persistent.UpdateReq_Settings) {
	protoSettings.MessageTimeout = &persistent.UpdateReq_Settings_MessageTimeoutMs{
		MessageTimeoutMs: c.MilliSeconds,
	}
}

type MessageTimeoutInTicks struct {
	Ticks int64
}

func (c MessageTimeoutInTicks) isCreateRequestMessageTimeout() {
}

func (c MessageTimeoutInTicks) buildCreateRequestSettings(
	protoSettings *persistent.CreateReq_Settings) {
	protoSettings.MessageTimeout = &persistent.CreateReq_Settings_MessageTimeoutTicks{
		MessageTimeoutTicks: c.Ticks,
	}
}

func (c MessageTimeoutInTicks) buildUpdateRequestSettings(
	protoSettings *persistent.UpdateReq_Settings) {
	protoSettings.MessageTimeout = &persistent.UpdateReq_Settings_MessageTimeoutTicks{
		MessageTimeoutTicks: c.Ticks,
	}
}

type isCreateRequestCheckpointAfter interface {
	isCreateRequestCheckpointAfter()
	buildCreateRequestSettings(*persistent.CreateReq_Settings)
	buildUpdateRequestSettings(*persistent.UpdateReq_Settings)
}

type CheckpointAfterTicks struct {
	Ticks int64
}

func (c CheckpointAfterTicks) isCreateRequestCheckpointAfter() {
}

func (c CheckpointAfterTicks) buildCreateRequestSettings(
	protoSettings *persistent.CreateReq_Settings) {
	protoSettings.CheckpointAfter = &persistent.CreateReq_Settings_CheckpointAfterTicks{
		CheckpointAfterTicks: c.Ticks,
	}
}

func (c CheckpointAfterTicks) buildUpdateRequestSettings(
	protoSettings *persistent.UpdateReq_Settings) {
	protoSettings.CheckpointAfter = &persistent.UpdateReq_Settings_CheckpointAfterTicks{
		CheckpointAfterTicks: c.Ticks,
	}
}

type CheckpointAfterMs struct {
	MilliSeconds int32
}

func (c CheckpointAfterMs) isCreateRequestCheckpointAfter() {
}

func (c CheckpointAfterMs) buildCreateRequestSettings(protoSettings *persistent.CreateReq_Settings) {
	protoSettings.CheckpointAfter = &persistent.CreateReq_Settings_CheckpointAfterMs{
		CheckpointAfterMs: c.MilliSeconds,
	}
}

func (c CheckpointAfterMs) buildUpdateRequestSettings(
	protoSettings *persistent.UpdateReq_Settings) {
	protoSettings.CheckpointAfter = &persistent.UpdateReq_Settings_CheckpointAfterMs{
		CheckpointAfterMs: c.MilliSeconds,
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
