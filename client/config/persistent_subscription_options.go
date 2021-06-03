package config

type SubscriptionConsumerStrategy int32
type FrequencyMode int32

const (
	SubscriptionDispatchToSingle SubscriptionConsumerStrategy = 0
	SubscriptionRoundRobin       SubscriptionConsumerStrategy = 1
	SubscriptionPinned           SubscriptionConsumerStrategy = 2

	FrequencyModeMs    FrequencyMode = 0
	FrequencyModeTicks FrequencyMode = 1
)

// Enum value maps for CreateReq_ConsumerStrategy.
var (
	SubscriptionConsumerStrategy_name = map[int32]string{
		0: "DispatchToSingle",
		1: "RoundRobin",
		2: "Pinned",
	}
	SubscriptionConsumerStrategy_value = map[string]int32{
		"DispatchToSingle": 0,
		"RoundRobin":       1,
		"Pinned":           2,
	}
	FrequencyMode_name = map[int32]string{
		0: "ModeMs",
		1: "ModeTicks",
	}
	FrequencyMode_value = map[string]int32{
		"ModeMs":    0,
		"ModeTicks": 1,
	}
)

type PersistentSubscriptionOptions struct {
	ResolveLinks          bool
	ExtraStatistics       bool
	MaxRetryCount         int32
	MinCheckpointCount    int32
	MaxCheckpointCount    int32
	MaxSubscriberCount    int32
	LiveBufferSize        int32
	ReadBatchSize         int32
	HistoryBufferSize     int32
	NamedConsumerStrategy SubscriptionConsumerStrategy

	CheckpointConfig FrequencyConfig

	MessageTimeoutConfig FrequencyConfig
}

type FrequencyConfig struct {
	Mode  FrequencyMode
	Value int64
}
