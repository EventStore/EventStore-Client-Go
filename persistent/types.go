package persistent

import (
	"math"

	"github.com/EventStore/EventStore-Client-Go/client/filtering"
	"github.com/EventStore/EventStore-Client-Go/position"
)

type ConsumerStrategy int32

const (
	ConsumerStrategy_DispatchToSingle ConsumerStrategy = 0
	ConsumerStrategy_RoundRobin       ConsumerStrategy = 1
	ConsumerStrategy_Pinned           ConsumerStrategy = 2
)

type Revision uint64

const (
	Revision_Start Revision = 0
	Revision_End   Revision = math.MaxUint64
)

type SubscriptionStreamConfig struct {
	StreamOption StreamSettings
	GroupName    string
	Settings     SubscriptionSettings
}

type SubscriptionAllOptionConfig struct {
	Position  position.Position
	Filter    *filtering.SubscriptionFilterOptions
	GroupName string
	Settings  SubscriptionSettings
}

type SubscriptionUpdateAllOptionConfig struct {
	Position  position.Position
	GroupName string
	Settings  SubscriptionSettings
}

type StreamSettings struct {
	StreamName []byte
	Revision   Revision
}

type SubscriptionSettings struct {
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
	MessageTimeoutInMs    int32
	CheckpointAfterInMs   int32
}
