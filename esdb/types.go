package esdb

import (
	"fmt"
	"strings"
	"time"
)

const SUBSCRIBER_COUNT_UNLIMITED = 0

type ConsumerStrategy int32

const (
	ConsumerStrategy_RoundRobin          ConsumerStrategy = 0
	ConsumerStrategy_DispatchToSingle    ConsumerStrategy = 1
	ConsumerStrategy_Pinned              ConsumerStrategy = 2
	ConsumerStrategy_PinnedByCorrelation ConsumerStrategy = 3
)

type SubscriptionSettings struct {
	ResolveLinkTos        bool
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

func SubscriptionSettingsDefault() SubscriptionSettings {
	return SubscriptionSettings{
		ResolveLinkTos:        false,
		ExtraStatistics:       false,
		MaxRetryCount:         10,
		MinCheckpointCount:    10,
		MaxCheckpointCount:    10 * 1000,
		MaxSubscriberCount:    SUBSCRIBER_COUNT_UNLIMITED,
		LiveBufferSize:        500,
		ReadBatchSize:         20,
		HistoryBufferSize:     500,
		NamedConsumerStrategy: ConsumerStrategy_RoundRobin,
		MessageTimeoutInMs:    30 * 1000,
		CheckpointAfterInMs:   2 * 1000,
	}
}

type PersistentSubscriptionError struct {
	Code int
	Err  error
}

var PersistentSubscriptionToAllMustProvideRegexOrPrefixError = PersistentSubscriptionError{
	Code: 0,
}

var PersistentSubscriptionToAllCanSetOnlyRegexOrPrefixError = PersistentSubscriptionError{
	Code: 1,
}

func PersistentSubscriptionFailedToInitClientError(err error) error {
	return &PersistentSubscriptionError{
		Code: 2,
		Err:  err,
	}
}

func PersistentSubscriptionFailedSendStreamInitError(err error) error {
	return &PersistentSubscriptionError{
		Code: 3,
		Err:  err,
	}
}

func PersistentSubscriptionFailedReceiveStreamInitError(err error) error {
	return &PersistentSubscriptionError{
		Code: 4,
		Err:  err,
	}
}

func PersistentSubscriptionNoConfirmationError(err error) error {
	return &PersistentSubscriptionError{
		Code: 5,
		Err:  err,
	}
}

func PersistentSubscriptionFailedCreationError(err error) error {
	return &PersistentSubscriptionError{
		Code: 6,
		Err:  err,
	}
}

func PersistentSubscriptionUpdateFailedError(err error) error {
	return &PersistentSubscriptionError{
		Code: 7,
		Err:  err,
	}
}

func PersistentSubscriptionDeletionFailedError(err error) error {
	return &PersistentSubscriptionError{
		Code: 8,
		Err:  err,
	}
}

var PersistentSubscriptionExceedsMaxMessageCountError = PersistentSubscriptionError{
	Code: 9,
}

func (e *PersistentSubscriptionError) Error() string {
	switch e.Code {
	case 0:
		return "the persistent subscription filter requires a set of prefixes or a regex"
	case 1:
		return "the persistent subscription filter may only contain a regex or a set of prefixes, but not both"
	case 2:
		return fmt.Sprintf("failed to init the persistent subscription esdb: %s", e.Err)
	case 3:
		return fmt.Sprintf("failed to init persistent subscription send stream: %s", e.Err)
	case 4:
		return fmt.Sprintf("failed to init persistent subscription receive stream: %s", e.Err)
	case 5:
		return fmt.Sprintf("persistent subscription received not confirmation: %s", e.Err)
	case 6:
		return fmt.Sprintf("failed to create persistent subscription: %s", e.Err)
	case 7:
		return fmt.Sprintf("failed to update persistent subscription: %s", e.Err)
	case 8:
		return fmt.Sprintf("failed to delete persistent subscription: %s", e.Err)
	case 9:
		return "persistent subscription max message count exceeds maximum value"
	default:
		return "unknown persistent subscription to all error"
	}
}

func (e *PersistentSubscriptionError) Is(target error) bool {
	t, ok := target.(*PersistentSubscriptionError)

	if !ok {
		return false
	}

	return e.Code == t.Code
}

func (e *PersistentSubscriptionError) Unwrap() error {
	return e.Err
}

// Position ...
type Position struct {
	Commit  uint64
	Prepare uint64
}

// EmptyPosition ...
var EmptyPosition Position = Position{Commit: ^uint64(0), Prepare: ^uint64(0)}

// StartPosition ...
var StartPosition Position = Position{Commit: 0, Prepare: 0}

// EndPosition ...
var EndPosition Position = Position{Commit: ^uint64(0), Prepare: ^uint64(0)}

// Direction ...
type Direction int

const (
	// Forwards ...
	Forwards Direction = iota
	// Backwards ...
	Backwards
)

const (
	UserStreamAcl   = "$userStreamAcl"
	SystemStreamAcl = "$systemStreamAcl"
)

type Acl struct {
	readRoles      []string
	writeRoles     []string
	deleteRoles    []string
	metaReadRoles  []string
	metaWriteRoles []string
}

func (a *Acl) AddReadRoles(roles ...string) {
	a.readRoles = append(a.readRoles, roles...)
}

func (a *Acl) AddWriteRoles(roles ...string) {
	a.writeRoles = append(a.writeRoles, roles...)
}

func (a *Acl) AddDeleteRoles(roles ...string) {
	a.deleteRoles = append(a.deleteRoles, roles...)
}

func (a *Acl) AddMetaWriteRoles(roles ...string) {
	a.metaWriteRoles = append(a.metaWriteRoles, roles...)
}

func (a *Acl) ReadRoles() []string {
	return a.readRoles
}

func (a *Acl) WriteRoles() []string {
	return a.writeRoles
}

func (a *Acl) DeleteRoles() []string {
	return a.deleteRoles
}

func (a *Acl) MetaReadRoles() []string {
	return a.metaReadRoles
}

func (a *Acl) MetaWriteRoles() []string {
	return a.metaWriteRoles
}

func (a *Acl) AddMetaReadRoles(roles ...string) {
	a.metaReadRoles = append(a.metaReadRoles, roles...)
}

type StreamMetadata struct {
	maxCount         []uint64
	maxAge           []time.Duration
	truncateBefore   []uint64
	cacheControl     []time.Duration
	acl              []interface{}
	customProperties map[string]interface{}
}

func (m *StreamMetadata) SetMaxCount(value uint64) {
	m.maxCount = []uint64{value}
}

func (m *StreamMetadata) SetMaxAge(value time.Duration) {
	m.maxAge = []time.Duration{value}
}

func (m *StreamMetadata) SetTruncateBefore(value uint64) {
	m.truncateBefore = []uint64{value}
}

func (m *StreamMetadata) SetCacheControl(value time.Duration) {
	m.cacheControl = []time.Duration{value}
}

func (m *StreamMetadata) SetAcl(value interface{}) {
	m.acl = []interface{}{value}
}

func (m *StreamMetadata) AddCustomProperty(name string, value interface{}) {
	if m.customProperties == nil {
		m.customProperties = make(map[string]interface{})
	}

	m.customProperties[name] = value
}

func (m *StreamMetadata) MaxCount() *uint64 {
	if len(m.maxCount) == 0 {
		return nil
	}

	return &m.maxCount[0]
}

func (m *StreamMetadata) MaxAge() *time.Duration {
	if len(m.maxAge) == 0 {
		return nil
	}

	return &m.maxAge[0]
}

func (m *StreamMetadata) TruncateBefore() *uint64 {
	if len(m.truncateBefore) == 0 {
		return nil
	}

	return &m.truncateBefore[0]
}

func (m *StreamMetadata) CacheControl() *time.Duration {
	if len(m.cacheControl) == 0 {
		return nil
	}

	return &m.cacheControl[0]
}

func (m *StreamMetadata) Acl() interface{} {
	if len(m.acl) == 0 {
		return nil
	}

	return m.acl[0]
}

func (m *StreamMetadata) StreamAcl() *Acl {
	acl := m.Acl()

	if acl != nil {
		if streamAcl, ok := acl.(Acl); ok {
			return &streamAcl
		}
	}

	return nil
}

func (m *StreamMetadata) IsUserStreamAcl() bool {
	acl := m.Acl()

	if acl != nil {
		if str, ok := acl.(string); ok {
			return str == UserStreamAcl
		}
	}

	return false
}

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

func (a Acl) ToMap() map[string]interface{} {
	props := make(map[string]interface{})

	flattenRoles(props, "$r", a.readRoles)
	flattenRoles(props, "$w", a.writeRoles)
	flattenRoles(props, "$d", a.deleteRoles)
	flattenRoles(props, "$mr", a.metaReadRoles)
	flattenRoles(props, "$mw", a.metaWriteRoles)

	return props
}

func AclFromMap(props map[string]interface{}) (Acl, error) {
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

func (m StreamMetadata) ToMap() (map[string]interface{}, error) {
	props := make(map[string]interface{})

	if maxCount := m.MaxCount(); maxCount != nil {
		props["$maxCount"] = *maxCount
	}

	if maxAge := m.MaxAge(); maxAge != nil {
		props["$maxAge"] = *maxAge
	}

	if truncateBefore := m.TruncateBefore(); truncateBefore != nil {
		props["$tb"] = *truncateBefore
	}

	if cacheControl := m.CacheControl(); cacheControl != nil {
		props["$cacheControl"] = *cacheControl
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
			props["$acl"] = value.ToMap()
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

func StreamMetadataFromMap(props map[string]interface{}) (StreamMetadata, error) {
	meta := StreamMetadata{}

	for key, value := range props {
		switch key {
		case "$maxCount":
			if i, ok := lookForUint64(value); ok {
				meta.SetMaxCount(i)
				continue
			}

			return meta, fmt.Errorf("invalid $maxCount value: %v", value)
		case "$maxAge":
			if ms, ok := lookForUint64(value); ok {
				meta.SetMaxAge(time.Duration(ms))
				continue
			}

			return meta, fmt.Errorf("invalid $maxAge value: %v", value)
		case "$tb":
			if i, ok := lookForUint64(value); ok {
				meta.SetTruncateBefore(i)
				continue
			}

			return meta, fmt.Errorf("invalid $tb value: %v", value)
		case "$cacheControl":
			if ms, ok := lookForUint64(value); ok {
				meta.SetCacheControl(time.Duration(ms))
				continue
			}

			return meta, fmt.Errorf("invalid $cacheControl value: %v, type: %t", value, value)
		case "$acl":
			switch aclValue := value.(type) {
			case string:
				if aclValue != UserStreamAcl && aclValue != SystemStreamAcl {
					return meta, fmt.Errorf("invalid string $acl value: %v", aclValue)
				}

				meta.SetAcl(value)
			case map[string]interface{}:
				acl, err := AclFromMap(aclValue)

				if err != nil {
					return meta, err
				}

				meta.SetAcl(acl)
			default:
				return meta, fmt.Errorf("invalid $acl object value: %v", value)
			}

		default:
			meta.AddCustomProperty(key, value)
		}
	}

	return meta, nil
}

type FilterType int

const (
	EventFilterType   FilterType = 0
	StreamFilterType  FilterType = 1
	NoMaxSearchWindow int        = -1
)

type SubscriptionFilter struct {
	Type     FilterType
	Prefixes []string
	Regex    string
}

func ExcludeSystemEventsFilter() *SubscriptionFilter {
	return &SubscriptionFilter{
		Type:  EventFilterType,
		Regex: "/^[^\\$].*/",
	}
}
