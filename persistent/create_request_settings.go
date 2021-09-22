package persistent

import "github.com/EventStore/EventStore-Client-Go/protos/persistent"

type CreateRequestSettings struct {
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
	MessageTimout         isCreateRequestMessageTimeout
	// Types that are assignable to MessageTimeout:
	//	*persistent.CreateReq_Settings_MessageTimeoutTicks
	//	*CreateReq_Settings_MessageTimeoutMs
	// MessageTimeout isCreateReq_Settings_MessageTimeout `protobuf_oneof:"message_timeout"`

	CheckpointAfter isCreateRequestCheckpointAfter
	// Types that are assignable to CheckpointAfter:
	//	*CreateReq_Settings_CheckpointAfterTicks
	//	*CreateReq_Settings_CheckpointAfterMs
	// CheckpointAfter isCreateReq_Settings_CheckpointAfter `protobuf_oneof:"checkpoint_after"`
}

func (settings CreateRequestSettings) buildCreateRequestSettings() *persistent.CreateReq_Settings {
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

	settings.MessageTimout.buildCreateRequestSettings(result)
	settings.CheckpointAfter.buildCreateRequestSettings(result)

	return result
}

func (settings CreateRequestSettings) buildUpdateRequestSettings() *persistent.UpdateReq_Settings {
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

	settings.MessageTimout.buildUpdateRequestSettings(result)
	settings.CheckpointAfter.buildUpdateRequestSettings(result)

	return result
}

var DefaultRequestSettings = CreateRequestSettings{
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
	MessageTimout: CreateRequestMessageTimeoutInMs{
		MilliSeconds: 30_000,
	},
	CheckpointAfter: CreateRequestCheckpointAfterMs{
		MilliSeconds: 2_000,
	},
}

type isCreateRequestMessageTimeout interface {
	isCreateRequestMessageTimeout()
	buildCreateRequestSettings(*persistent.CreateReq_Settings)
	buildUpdateRequestSettings(*persistent.UpdateReq_Settings)
}

type CreateRequestMessageTimeoutInMs struct {
	MilliSeconds int32
}

func (c CreateRequestMessageTimeoutInMs) isCreateRequestMessageTimeout() {
}

func (c CreateRequestMessageTimeoutInMs) buildCreateRequestSettings(
	protoSettings *persistent.CreateReq_Settings) {
	protoSettings.MessageTimeout = &persistent.CreateReq_Settings_MessageTimeoutMs{
		MessageTimeoutMs: c.MilliSeconds,
	}
}

func (c CreateRequestMessageTimeoutInMs) buildUpdateRequestSettings(
	protoSettings *persistent.UpdateReq_Settings) {
	protoSettings.MessageTimeout = &persistent.UpdateReq_Settings_MessageTimeoutMs{
		MessageTimeoutMs: c.MilliSeconds,
	}
}

type CreateRequestMessageTimeoutInTicks struct {
	Ticks int64
}

func (c CreateRequestMessageTimeoutInTicks) isCreateRequestMessageTimeout() {
}

func (c CreateRequestMessageTimeoutInTicks) buildCreateRequestSettings(
	protoSettings *persistent.CreateReq_Settings) {
	protoSettings.MessageTimeout = &persistent.CreateReq_Settings_MessageTimeoutTicks{
		MessageTimeoutTicks: c.Ticks,
	}
}

func (c CreateRequestMessageTimeoutInTicks) buildUpdateRequestSettings(
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

type CreateRequestCheckpointAfterTicks struct {
	Ticks int64
}

func (c CreateRequestCheckpointAfterTicks) isCreateRequestCheckpointAfter() {
}

func (c CreateRequestCheckpointAfterTicks) buildCreateRequestSettings(
	protoSettings *persistent.CreateReq_Settings) {
	protoSettings.CheckpointAfter = &persistent.CreateReq_Settings_CheckpointAfterTicks{
		CheckpointAfterTicks: c.Ticks,
	}
}

func (c CreateRequestCheckpointAfterTicks) buildUpdateRequestSettings(
	protoSettings *persistent.UpdateReq_Settings) {
	protoSettings.CheckpointAfter = &persistent.UpdateReq_Settings_CheckpointAfterTicks{
		CheckpointAfterTicks: c.Ticks,
	}
}

type CreateRequestCheckpointAfterMs struct {
	MilliSeconds int32
}

func (c CreateRequestCheckpointAfterMs) isCreateRequestCheckpointAfter() {
}

func (c CreateRequestCheckpointAfterMs) buildCreateRequestSettings(protoSettings *persistent.CreateReq_Settings) {
	protoSettings.CheckpointAfter = &persistent.CreateReq_Settings_CheckpointAfterMs{
		CheckpointAfterMs: c.MilliSeconds,
	}
}

func (c CreateRequestCheckpointAfterMs) buildUpdateRequestSettings(
	protoSettings *persistent.UpdateReq_Settings) {
	protoSettings.CheckpointAfter = &persistent.UpdateReq_Settings_CheckpointAfterMs{
		CheckpointAfterMs: c.MilliSeconds,
	}
}
