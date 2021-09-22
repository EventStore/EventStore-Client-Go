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

func (settings CreateRequestSettings) build() *persistent.CreateReq_Settings {
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

	settings.MessageTimout.build(result)
	settings.CheckpointAfter.build(result)

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
	build(*persistent.CreateReq_Settings)
}

type CreateRequestMessageTimeoutInMs struct {
	MilliSeconds int32
}

func (c CreateRequestMessageTimeoutInMs) isCreateRequestMessageTimeout() {
}

func (c CreateRequestMessageTimeoutInMs) build(protoSettings *persistent.CreateReq_Settings) {
	protoSettings.MessageTimeout = &persistent.CreateReq_Settings_MessageTimeoutMs{
		MessageTimeoutMs: c.MilliSeconds,
	}
}

type CreateRequestMessageTimeoutInTicks struct {
	Ticks int64
}

func (c CreateRequestMessageTimeoutInTicks) isCreateRequestMessageTimeout() {
}

func (c CreateRequestMessageTimeoutInTicks) build(protoSettings *persistent.CreateReq_Settings) {
	protoSettings.MessageTimeout = &persistent.CreateReq_Settings_MessageTimeoutTicks{
		MessageTimeoutTicks: c.Ticks,
	}
}

type isCreateRequestCheckpointAfter interface {
	isCreateRequestCheckpointAfter()
	build(*persistent.CreateReq_Settings)
}

type CreateRequestCheckpointAfterTicks struct {
	Ticks int64
}

func (c CreateRequestCheckpointAfterTicks) isCreateRequestCheckpointAfter() {
}

func (c CreateRequestCheckpointAfterTicks) build(protoSettings *persistent.CreateReq_Settings) {
	protoSettings.CheckpointAfter = &persistent.CreateReq_Settings_CheckpointAfterTicks{
		CheckpointAfterTicks: c.Ticks,
	}
}

type CreateRequestCheckpointAfterMs struct {
	MilliSeconds int32
}

func (c CreateRequestCheckpointAfterMs) isCreateRequestCheckpointAfter() {
}

func (c CreateRequestCheckpointAfterMs) build(protoSettings *persistent.CreateReq_Settings) {
	protoSettings.CheckpointAfter = &persistent.CreateReq_Settings_CheckpointAfterMs{
		CheckpointAfterMs: c.MilliSeconds,
	}
}
