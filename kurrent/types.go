package kurrent

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const SubscriberCountUnlimited = 0

// ConsumerStrategy named consumer strategies for use with persistent subscriptions.
type ConsumerStrategy string

const (
	// ConsumerStrategyRoundRobin Distributes events to all clients evenly. If the client buffer-size is reached, the
	// client is ignored until events are (not) acknowledged.
	ConsumerStrategyRoundRobin ConsumerStrategy = "RoundRobin"

	// ConsumerStrategyDispatchToSingle Distributes events to a single client until the buffer size is reached. After
	// which the next client is selected in a round-robin style, and the process is repeated.
	ConsumerStrategyDispatchToSingle ConsumerStrategy = "DispatchToSingle"

	// ConsumerStrategyPinned For use with an indexing projection such as the system '$by_category' projection.
	// KurrentDB inspects event for its source stream id, hashing the id to one of 1024 buckets assigned to
	// individual clients. When a client disconnects, its buckets are assigned to other clients. When a client connects,
	// it is assigned some existing buckets. This naively attempts to maintain a balanced workload. The main goal of
	// this strategy is to decrease the likelihood of concurrency and ordering issues while maintaining load balancing.
	// This is not a guarantee, and you should handle the usual ordering and concurrency issues.
	ConsumerStrategyPinned ConsumerStrategy = "Pinned"
)

// PersistentSubscriptionSettings persistent subscription settings.
type PersistentSubscriptionSettings struct {
	// Where to start the subscription from.
	StartFrom interface{}
	// Resolve linkTo event to their linked events.
	ResolveLinkTos bool
	// Enable tracking of latency statistics on this subscription.
	ExtraStatistics bool
	// The maximum number of retries (due to timeout) before a message is considered to be parked. Default: 10.
	MaxRetryCount int32
	// The minimum number of messages to process before a checkpoint may be written. Default: 10.
	CheckpointLowerBound int32
	// The maximum number of messages not checkpointed before forcing a checkpoint. Default: 1000.
	CheckpointUpperBound int32
	// The maximum number of subscribers allowed. Default: 0 (Unbounded).
	MaxSubscriberCount int32
	// The size of the buffer (in-memory) listening to live messages as they happen before paging occurs. Default: 500.
	LiveBufferSize int32
	// The number of events read at a time when catching up. Default: 20.
	ReadBatchSize int32
	// The number of events to cache when catching up. Default: 500.
	HistoryBufferSize int32
	// The strategy to use for distributing events to client consumers.
	ConsumerStrategyName ConsumerStrategy
	// The amount of time after which to consider a message as timed out and retried. Default 30_000ms.
	MessageTimeout int32
	// The amount of time to try to checkpoint after. Default: 2_000ms.
	CheckpointAfter int32
}

// SubscriptionSettingsDefault returns a persistent subscription settings with default values.
func SubscriptionSettingsDefault() PersistentSubscriptionSettings {
	return PersistentSubscriptionSettings{
		ResolveLinkTos:       false,
		ExtraStatistics:      false,
		MaxRetryCount:        10,
		CheckpointLowerBound: 10,
		CheckpointUpperBound: 1_000,
		MaxSubscriberCount:   SubscriberCountUnlimited,
		LiveBufferSize:       500,
		ReadBatchSize:        20,
		HistoryBufferSize:    500,
		ConsumerStrategyName: ConsumerStrategyRoundRobin,
		MessageTimeout:       30 * 1000,
		CheckpointAfter:      2 * 1000,
	}
}

// Position transaction log position.
type Position struct {
	// Commit position.
	Commit uint64
	// Prepare position.
	Prepare uint64
}

// Direction Read direction.
type Direction int

const (
	// Forwards Reads from the start to the end.
	Forwards Direction = iota
	// Backwards Reads from the end to the start.
	Backwards
)

const (
	// UserStreamAcl Default system ACL.
	UserStreamAcl = "$userStreamAcl"
	// SystemStreamAcl Default users ACL.
	SystemStreamAcl = "$systemStreamAcl"
)

// Acl Access Control List (ACL).
type Acl struct {
	readRoles      []string
	writeRoles     []string
	deleteRoles    []string
	metaReadRoles  []string
	metaWriteRoles []string
}

// AddReadRoles Adds read roles.
func (a *Acl) AddReadRoles(roles ...string) {
	a.readRoles = append(a.readRoles, roles...)
}

// AddWriteRoles Adds write roles.
func (a *Acl) AddWriteRoles(roles ...string) {
	a.writeRoles = append(a.writeRoles, roles...)
}

// AddDeleteRoles Adds delete roles.
func (a *Acl) AddDeleteRoles(roles ...string) {
	a.deleteRoles = append(a.deleteRoles, roles...)
}

// AddMetaWriteRoles Adds metadata write roles.
func (a *Acl) AddMetaWriteRoles(roles ...string) {
	a.metaWriteRoles = append(a.metaWriteRoles, roles...)
}

// ReadRoles Returns read roles.
func (a *Acl) ReadRoles() []string {
	return a.readRoles
}

// WriteRoles Returns write roles.
func (a *Acl) WriteRoles() []string {
	return a.writeRoles
}

// DeleteRoles Returns delete roles.
func (a *Acl) DeleteRoles() []string {
	return a.deleteRoles
}

// MetaReadRoles Returns metadata read roles.
func (a *Acl) MetaReadRoles() []string {
	return a.metaReadRoles
}

// MetaWriteRoles Returns metadata write roles.
func (a *Acl) MetaWriteRoles() []string {
	return a.metaWriteRoles
}

// AddMetaReadRoles Adds metadata roles.
func (a *Acl) AddMetaReadRoles(roles ...string) {
	a.metaReadRoles = append(a.metaReadRoles, roles...)
}

// StreamMetadata Represents stream metadata with strongly typed properties for system values and a dictionary-like
// interface for custom values.
type StreamMetadata struct {
	maxCount         []uint64
	maxAge           []time.Duration
	truncateBefore   []uint64
	cacheControl     []time.Duration
	acl              []interface{}
	customProperties map[string]interface{}
}

// SetMaxCount The maximum number of events allowed in the stream.
func (m *StreamMetadata) SetMaxCount(value uint64) {
	m.maxCount = []uint64{value}
}

// SetMaxAge The maximum age of events allowed in the stream.
func (m *StreamMetadata) SetMaxAge(value time.Duration) {
	m.maxAge = []time.Duration{value}
}

// SetTruncateBefore The event number from which previous events can be scavenged. This is used to implement
// deletions of streams.
func (m *StreamMetadata) SetTruncateBefore(value uint64) {
	m.truncateBefore = []uint64{value}
}

// SetCacheControl The amount of time for which the stream head is cacheable (in seconds).
func (m *StreamMetadata) SetCacheControl(value time.Duration) {
	m.cacheControl = []time.Duration{value}
}

// SetAcl The Access Control List of the stream (ACL).
func (m *StreamMetadata) SetAcl(value interface{}) {
	m.acl = []interface{}{value}
}

// AddCustomProperty Key-value pair of a key to JSON for user-provider metadata.
func (m *StreamMetadata) AddCustomProperty(name string, value interface{}) {
	if m.customProperties == nil {
		m.customProperties = make(map[string]interface{})
	}

	m.customProperties[name] = value
}

// MaxCount The maximum number of events allowed in the stream.
func (m *StreamMetadata) MaxCount() *uint64 {
	if len(m.maxCount) == 0 {
		return nil
	}

	return &m.maxCount[0]
}

// MaxAge The maximum age of events allowed in the stream.
func (m *StreamMetadata) MaxAge() *time.Duration {
	if len(m.maxAge) == 0 {
		return nil
	}

	return &m.maxAge[0]
}

// TruncateBefore The event number from which previous events can be scavenged. This is used to implement deletions
// of streams.
func (m *StreamMetadata) TruncateBefore() *uint64 {
	if len(m.truncateBefore) == 0 {
		return nil
	}

	return &m.truncateBefore[0]
}

// CacheControl The amount of time for which the stream head is cacheable (in seconds).
func (m *StreamMetadata) CacheControl() *time.Duration {
	if len(m.cacheControl) == 0 {
		return nil
	}

	return &m.cacheControl[0]
}

// Acl The Access Control List of the stream (ACL).
func (m *StreamMetadata) Acl() interface{} {
	if len(m.acl) == 0 {
		return nil
	}

	return m.acl[0]
}

// StreamAcl The Access Control List of the stream (ACL).
func (m *StreamMetadata) StreamAcl() *Acl {
	acl := m.Acl()

	if acl != nil {
		if streamAcl, ok := acl.(Acl); ok {
			return &streamAcl
		}
	}

	return nil
}

// CustomProperty The custom property value for the given key.
func (m *StreamMetadata) CustomProperty(key string) interface{} {
	return m.customProperties[key]
}

// CustomProperties returns all custom properties.
func (m *StreamMetadata) CustomProperties() map[string]interface{} {
	return m.customProperties
}

// IsUserStreamAcl Checks if the ACL is set to users default.
func (m *StreamMetadata) IsUserStreamAcl() bool {
	acl := m.Acl()

	if acl != nil {
		if str, ok := acl.(string); ok {
			return str == UserStreamAcl
		}
	}

	return false
}

// IsSystemStreamAcl Checks if the ACL is set to system default.
func (m *StreamMetadata) IsSystemStreamAcl() bool {
	acl := m.Acl()

	if acl != nil {
		if str, ok := acl.(string); ok {
			return str == SystemStreamAcl
		}
	}

	return false
}

func flattenRoles(props map[string]interface{}, key string, roles []string) {
	len_r := len(roles)

	if len_r == 0 {
		return
	}

	if len_r == 1 {
		props[key] = roles[0]
		return
	}

	props[key] = roles
}

func collectRoles(value interface{}) ([]string, error) {

	switch roleValue := value.(type) {
	case string:
		return []string{roleValue}, nil
	case []string:
		return roleValue, nil
	default:
		return nil, fmt.Errorf("invalid acl role value: %v", roleValue)
	}
}

func (a Acl) toMap() map[string]interface{} {
	props := make(map[string]interface{})

	flattenRoles(props, "$r", a.readRoles)
	flattenRoles(props, "$w", a.writeRoles)
	flattenRoles(props, "$d", a.deleteRoles)
	flattenRoles(props, "$mr", a.metaReadRoles)
	flattenRoles(props, "$mw", a.metaWriteRoles)

	return props
}

func aclFromMap(props map[string]interface{}) (Acl, error) {
	acl := Acl{}

	for key, value := range props {
		switch key {
		case "$r":
			roles, err := collectRoles(value)

			if err != nil {
				return acl, err
			}

			acl.readRoles = roles
		case "$w":
			roles, err := collectRoles(value)

			if err != nil {
				return acl, err
			}

			acl.writeRoles = roles
		case "$d":
			roles, err := collectRoles(value)

			if err != nil {
				return acl, err
			}

			acl.deleteRoles = roles
		case "$mr":
			roles, err := collectRoles(value)

			if err != nil {
				return acl, err
			}

			acl.metaReadRoles = roles
		case "$mw":
			roles, err := collectRoles(value)

			if err != nil {
				return acl, err
			}

			acl.metaWriteRoles = roles
		default:
			return acl, fmt.Errorf("unknown acl key: %v", key)
		}
	}

	return acl, nil
}

func (m *StreamMetadata) toMap() (map[string]interface{}, error) {
	props := make(map[string]interface{})

	if maxCount := m.MaxCount(); maxCount != nil {
		props["$maxCount"] = *maxCount
	}

	if maxAge := m.MaxAge(); maxAge != nil {
		props["$maxAge"] = int64(maxAge.Seconds())
	}

	if truncateBefore := m.TruncateBefore(); truncateBefore != nil {
		props["$tb"] = *truncateBefore
	}

	if cacheControl := m.CacheControl(); cacheControl != nil {
		props["$cacheControl"] = int64(cacheControl.Seconds())
	}

	acl := m.Acl()
	if acl != nil {
		switch value := acl.(type) {
		case string:
			if value != UserStreamAcl && value != SystemStreamAcl {
				return nil, fmt.Errorf("unsupported acl string value: %s", value)
			}

			props["$acl"] = value
		case Acl:
			props["$acl"] = value.toMap()
		}
	}

	for key, value := range m.customProperties {
		// We ignore properties that can conflict with internal metatadata names.
		if strings.HasPrefix(key, "$") {
			continue
		}

		props[key] = value
	}

	return props, nil
}

// ToJson serializes StreamMetadata to JSON.
func (m *StreamMetadata) ToJson() ([]byte, error) {
	mm, err := m.toMap()

	if err != nil {
		return nil, err
	}

	return json.Marshal(mm)
}

// StreamMetadataFromJson deserializes a JSON byte array into a StreamMetadata object.
func StreamMetadataFromJson(bytes []byte) (*StreamMetadata, error) {
	var outProps map[string]interface{}
	err := json.Unmarshal(bytes, &outProps)

	if err != nil {
		return nil, err
	}

	return streamMetadataFromMap(outProps)
}

func lookForUint64(value interface{}) (uint64, bool) {
	if i, ok := value.(uint64); ok {
		return i, true
	}

	if i, ok := value.(uint32); ok {
		return uint64(i), true
	}

	if i, ok := value.(float64); ok {
		return uint64(i), true
	}

	return 0, false
}

func streamMetadataFromMap(props map[string]interface{}) (*StreamMetadata, error) {
	meta := StreamMetadata{}

	for key, value := range props {
		switch key {
		case "$maxCount":
			if i, ok := lookForUint64(value); ok {
				meta.SetMaxCount(i)
				continue
			}

			return nil, fmt.Errorf("invalid $maxCount value: %v", value)
		case "$maxAge":
			if secs, ok := lookForUint64(value); ok {
				meta.SetMaxAge(time.Duration(secs) * time.Second)
				continue
			}

			return nil, fmt.Errorf("invalid $maxAge value: %v", value)
		case "$tb":
			if i, ok := lookForUint64(value); ok {
				meta.SetTruncateBefore(i)
				continue
			}

			return nil, fmt.Errorf("invalid $tb value: %v", value)
		case "$cacheControl":
			if secs, ok := lookForUint64(value); ok {
				meta.SetCacheControl(time.Duration(secs) * time.Second)
				continue
			}

			return nil, fmt.Errorf("invalid $cacheControl value: %v, type: %t", value, value)
		case "$acl":
			switch aclValue := value.(type) {
			case string:
				if aclValue != UserStreamAcl && aclValue != SystemStreamAcl {
					return nil, fmt.Errorf("invalid string $acl value: %v", aclValue)
				}

				meta.SetAcl(value)
			case map[string]interface{}:
				acl, err := aclFromMap(aclValue)

				if err != nil {
					return nil, err
				}

				meta.SetAcl(acl)
			default:
				return nil, fmt.Errorf("invalid $acl object value: %v", value)
			}

		default:
			meta.AddCustomProperty(key, value)
		}
	}

	return &meta, nil
}

// FilterType  represents the type filters supported by KurrentDB.
type FilterType int

const (
	// EventFilterType the filter will be applied on an event's type.
	EventFilterType FilterType = 0
	// StreamFilterType the filter will be applied on a stream name.
	StreamFilterType FilterType = 1
	// NoMaxSearchWindow disables the max search window.
	NoMaxSearchWindow int = -1
)

// SubscriptionFilter is a filter that targets $all stream.
type SubscriptionFilter struct {
	// Type of filter.
	Type FilterType
	// String prefixes.
	Prefixes []string
	// Regex expression.
	Regex string
}

// ExcludeSystemEventsFilter filters out event's with system's event types, i.e. event types starting with `$`.
func ExcludeSystemEventsFilter() *SubscriptionFilter {
	return &SubscriptionFilter{
		Type:  EventFilterType,
		Regex: "^[^\\$].*",
	}
}

type persistentSubscriptionInfoHttpJson struct {
	EventStreamId                 string                                 `json:"eventStreamId"`
	GroupName                     string                                 `json:"groupName"`
	Status                        string                                 `json:"status"`
	AverageItemsPerSecond         float64                                `json:"averageItemsPerSecond"`
	TotalItemsProcessed           int64                                  `json:"totalItemsProcessed"`
	LastProcessedEventNumber      int64                                  `json:"lastProcessedEventNumber"`
	LastKnownEventNumber          int64                                  `json:"lastKnownEventNumber"`
	LastCheckpointedEventPosition string                                 `json:"lastCheckpointedEventPosition,omitempty"`
	LastKnownEventPosition        string                                 `json:"lastKnownEventPosition,omitempty"`
	ConnectionCount               int64                                  `json:"connectionCount,omitempty"`
	TotalInFlightMessages         int64                                  `json:"totalInFlightMessages"`
	Config                        *persistentSubscriptionConfig          `json:"config,omitempty"`
	Connections                   []PersistentSubscriptionConnectionInfo `json:"connections,omitempty"`
	ReadBufferCount               int64                                  `json:"readBufferCount"`
	RetryBufferCount              int64                                  `json:"retryBufferCount"`
	LiveBufferCount               int64                                  `json:"liveBufferCount"`
	OutstandingMessagesCount      int64                                  `json:"OutstandingMessagesCount"`
	ParkedMessageCount            int64                                  `json:"parkedMessageCount"`
	CountSinceLastMeasurement     int64                                  `json:"countSinceLastMeasurement"`
}

// PersistentSubscriptionInfo represents persistent subscription info.
type PersistentSubscriptionInfo struct {
	// The source of events for the subscription.
	EventSource string
	// The group name given on creation.
	GroupName string
	// The current status of the subscription.
	Status string
	// Active connections to the subscription.
	Connections []PersistentSubscriptionConnectionInfo
	// Persistent subscription's settings.
	Settings *PersistentSubscriptionSettings
	// Persistent subscription's stats.
	Stats *PersistentSubscriptionStats
}

// PersistentSubscriptionStats represents processing-related persistent subscription statistics.
type PersistentSubscriptionStats struct {
	// Average number of events per seconds.
	AveragePerSecond int64
	// Total number of events processed by this subscription.
	TotalItems int64
	// Number of events seen since last measurement on this subscription.
	CountSinceLastMeasurement int64
	// The revision number of the last checkpoint.
	LastCheckpointedEventRevision *uint64
	// The revision number of the last known.
	LastKnownEventRevision *uint64
	// The transaction log position of the last checkpoint.
	LastCheckpointedPosition *Position
	// The transaction log position of the last known event.
	LastKnownPosition *Position
	// Number of events in the read buffer.
	ReadBufferCount int64
	// Number of events in the live buffer.
	LiveBufferCount int64
	// Number of events in the retry buffer.
	RetryBufferCount int64
	// Current in flight messages across the persistent subscription group.
	TotalInFlightMessages int64
	// Current number of outstanding messages.
	OutstandingMessagesCount int64
	// The current number of parked messages.
	ParkedMessagesCount int64
}

type persistentSubscriptionConfig struct {
	ResolveLinkTos       bool   `json:"resolveLinktos"`
	StartFrom            int64  `json:"startFrom"`
	StartPosition        string `json:"startPosition,omitempty"`
	MessageTimeout       int64  `json:"messageTimeoutMilliseconds"`
	ExtraStatistics      bool   `json:"extraStatistics"`
	MaxRetryCount        int64  `json:"maxRetryCount"`
	LiveBufferSize       int64  `json:"liveBufferSize"`
	BufferSize           int64  `json:"bufferSize"`
	ReadBatchSize        int64  `json:"readBatchSize"`
	PreferRoundRobin     bool   `json:"preferRoundRobin"`
	CheckpointAfter      int64  `json:"checkPointAfterMilliseconds"`
	CheckpointLowerBound int64  `json:"minCheckPointCount"`
	CheckpointUpperBound int64  `json:"maxCheckPointCount"`
	MaxSubscriberCount   int64  `json:"maxSubscriberCount"`
	ConsumerStrategyName string `json:"consumerStrategyName"`
}

// PersistentSubscriptionConnectionInfo holds an active persistent subscription connection info.
type PersistentSubscriptionConnectionInfo struct {
	// Origin of this connection.
	From string `json:"from"`
	// Connection's username.
	Username string `json:"username"`
	// Average events per second on this connection.
	AverageItemsPerSecond float64 `json:"averageItemsPerSecond"`
	// Total items on this connection.
	TotalItemsProcessed int64 `json:"totalItemsProcessed"`
	// Number of items seen since last measurement on this connection.
	CountSinceLastMeasurement int64 `json:"countSinceLastMeasurement"`
	// Number of available slots.
	AvailableSlots int64 `json:"availableSlots"`
	// Number of in flight messages on this connection.
	InFlightMessages int64 `json:"inFlightMessages"`
	// Connection's name.
	ConnectionName string `json:"connectionName"`
	// Timing measurements for the connection.
	ExtraStatistics []PersistentSubscriptionMeasurement `json:"extraStatistics"`
}

// PersistentSubscriptionMeasurement key-value pair of a metric and its value.
type PersistentSubscriptionMeasurement struct {
	// Metric name
	Key string `json:"key"`
	// Metric value.
	Value int64 `json:"value"`
}
