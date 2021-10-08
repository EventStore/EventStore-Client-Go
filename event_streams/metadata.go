package event_streams

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"github.com/google/uuid"
)

// StreamMetadataResult is stream's metadata read by Client.GetStreamMetadata.
//
// Streams metadata hold information about a stream. Some of that information is an
// access control list (acl) for a stream. See StreamMetadata for more info.
//
// Stream does not need to have metadata set. If stream has no metadata set than IsEmpty returns false.
type StreamMetadataResult struct {
	streamId string
	result   isStreamMetadataResult
}

// IsEmpty returns true if stream's metadata stream has nothing stored in it.
func (result StreamMetadataResult) IsEmpty() bool {
	return result.result == nil
}

// GetStreamId returns a stream's identifier to which metadata relates to.
func (result StreamMetadataResult) GetStreamId() string {
	return result.streamId
}

// GetStreamMetadata returns stream's latest metadata.
// If stream has no metadata set it returns a zero initialized StreamMetadata.
func (result StreamMetadataResult) GetStreamMetadata() StreamMetadata {
	if result.result != nil {
		if result, ok := result.result.(metadataResult); ok {
			return result.streamMetadata
		}
	}

	return StreamMetadata{}
}

// GetMetaStreamRevision returns a current revision of stream's metadata stream if stream has metadata set.
// If stream has no metadata set it returns 0. Use IsEmpty to determine if stream has any metadata set.
func (result StreamMetadataResult) GetMetaStreamRevision() uint64 {
	if result.result != nil {
		if result, ok := result.result.(metadataResult); ok {
			return result.metaStreamRevision
		}
	}

	return 0
}

type isStreamMetadataResult interface {
	isStreamMetadataResult()
}

// metadataResult is stream's metadata together with stream ID and current revision of
// stream's metadata stream.
type metadataResult struct {
	streamMetadata     StreamMetadata
	metaStreamRevision uint64
}

func (result metadataResult) isStreamMetadataResult() {}

func newStreamMetadataResultImpl(streamId string, event ResolvedEvent) StreamMetadataResult {
	var metaData StreamMetadata
	if err := json.Unmarshal(event.Event.Data, &metaData); err != nil {
		panic(err)
	}

	return StreamMetadataResult{
		streamId: streamId,
		result: metadataResult{
			streamMetadata:     metaData,
			metaStreamRevision: event.Event.EventNumber,
		},
	}
}

const StreamMetadataType = "$metadata"

const (
	maxAgeJsonProperty         = "$maxAge"
	truncateBeforeJsonProperty = "$tb"
	cacheControlJsonProperty   = "$cacheControl"
	aclJsonProperty            = "$acl"
	maxCountJsonProperty       = "$maxCount"
)

// CustomMetadataType is shorthand type for user defined metadata of a stream.
type CustomMetadataType map[string]interface{}

// StreamMetadata is the metadata of a stream.
// You can read more about stream metadata at
// https://developers.eventstore.com/server/v21.6/streams/metadata-and-reserved-names.html#reserved-names
type StreamMetadata struct {
	// MaxAgeInSeconds Sets a sliding window based on dates.
	// When data reaches a certain age it disappears automatically from the stream and is
	// considered eligible for scavenging.
	// This value is set as an integer representing the number of seconds. This value must be >= 1.
	MaxAgeInSeconds *uint64 `json:"$maxAge"`
	// TruncateBefore indicates a stream's revision before all events in a stream are truncated from
	// stream read operations.
	// If TruncateBefore is 4 that means that all events before revision 4 are truncated from stream
	// read operation.
	// Truncation naturally occurs when a soft-delete is performed on a stream with Client.DeleteStream.
	TruncateBefore *uint64 `json:"$tb"`
	// This controls the cache of the head of a stream.
	// Most URIs in a stream are infinitely cacheable but the head by default will not cache.
	// It may be preferable in some situations to set a small amount of caching on the head to
	// allow intermediaries to handle polls (say 10 seconds).
	// The argument is an integer representing the seconds to cache. This value must be >= 1.
	CacheControlInSeconds *uint64 `json:"$cacheControl"`
	// Access Control List for a stream.
	Acl *StreamAcl `json:"$acl"`
	// Sets a sliding window based on the number of items in the stream.
	// When data reaches a certain length it disappears automatically from the stream and is
	// considered eligible for scavenging.
	// This value is set as an integer representing the count of items. This value must be >= 1.
	MaxCount *int `json:"$maxCount"`
	// User defined metadata for a stream.
	CustomMetadata CustomMetadataType
}

// MarshalJSON implements JSON marshaller interface
func (b StreamMetadata) MarshalJSON() ([]byte, error) {
	dat := map[string]interface{}{}

	for key, value := range b.CustomMetadata {
		if !isCustomMetaValueAllowed(value) {
			return nil, errors.New(fmt.Sprintf("custom meta data value at key=%s is not allowed", key))
		}
		dat[key] = value
	}

	dat[maxAgeJsonProperty] = b.MaxAgeInSeconds
	dat[truncateBeforeJsonProperty] = b.TruncateBefore
	dat[cacheControlJsonProperty] = b.CacheControlInSeconds
	dat[maxCountJsonProperty] = b.MaxCount
	dat[aclJsonProperty] = b.Acl

	return json.Marshal(dat)
}

// UnmarshalJSON implements JSON marshaller interface
func (b *StreamMetadata) UnmarshalJSON(data []byte) error {
	var rawDataMap map[string]*json.RawMessage
	if err := json.Unmarshal(data, &rawDataMap); err != nil {
		panic(err)
	}

	if rawDataMap[maxAgeJsonProperty] != nil {
		var temp uint64

		stdErr := json.Unmarshal(*rawDataMap[maxAgeJsonProperty], &temp)
		if stdErr != nil {
			return stdErr
		}

		b.MaxAgeInSeconds = &temp
	}

	if rawDataMap[truncateBeforeJsonProperty] != nil {
		var temp uint64
		stdErr := json.Unmarshal(*rawDataMap[truncateBeforeJsonProperty], &temp)
		if stdErr != nil {
			return stdErr
		}

		b.TruncateBefore = &temp
	}

	if rawDataMap[cacheControlJsonProperty] != nil {
		var temp uint64
		stdErr := json.Unmarshal(*rawDataMap[cacheControlJsonProperty], &temp)
		if stdErr != nil {
			return stdErr
		}

		b.CacheControlInSeconds = &temp
	}

	if rawDataMap[maxCountJsonProperty] != nil {
		var temp int
		stdErr := json.Unmarshal(*rawDataMap[maxCountJsonProperty], &temp)
		if stdErr != nil {
			return stdErr
		}

		b.MaxCount = &temp
	}

	if rawDataMap[aclJsonProperty] != nil {
		var temp StreamAcl
		stdErr := json.Unmarshal(*rawDataMap[aclJsonProperty], &temp)
		if stdErr != nil {
			return stdErr
		}

		b.Acl = &temp
	}

	rawUserMetaDataMap := rawDataMap
	delete(rawUserMetaDataMap, maxCountJsonProperty)
	delete(rawUserMetaDataMap, cacheControlJsonProperty)
	delete(rawUserMetaDataMap, truncateBeforeJsonProperty)
	delete(rawUserMetaDataMap, maxAgeJsonProperty)
	delete(rawUserMetaDataMap, aclJsonProperty)

	if len(rawUserMetaDataMap) > 0 {
		userMetaDataBytes, stdErr := json.Marshal(rawUserMetaDataMap)

		if stdErr != nil {
			return stdErr
		}

		var userMetaData map[string]interface{}

		if !areAllCustomMetaValuesAllowed(userMetaData) {
			return errors.New("found custom meta data value which is not allowed")
		}

		stdErr = json.Unmarshal(userMetaDataBytes, &userMetaData)

		if stdErr != nil {
			return stdErr
		}

		b.CustomMetadata = userMetaData
	}

	return nil
}

func areAllCustomMetaValuesAllowed(userMetaData map[string]interface{}) bool {
	for _, value := range userMetaData {
		if !isCustomMetaValueAllowed(value) {
			return false
		}
	}

	return true
}

func isCustomMetaValueAllowed(value interface{}) bool {
	valueType := reflect.TypeOf(value)

	switch valueType.Kind() {
	case reflect.Array:
		fallthrough
	case reflect.Chan:
		fallthrough
	case reflect.Complex64:
		fallthrough
	case reflect.Complex128:
		fallthrough
	case reflect.Func:
		fallthrough
	case reflect.Map:
		fallthrough
	case reflect.Struct:
		fallthrough
	case reflect.Ptr:
		fallthrough
	case reflect.Slice:
		return false
	default:
		return true
	}
}

func getMetaStreamOf(streamName string) string {
	return "$$" + streamName
}

func newMetadataEvent(metadata StreamMetadata) ProposedEvent {
	eventId, _ := uuid.NewRandom()

	jsonBytes, stdErr := json.Marshal(metadata)

	if stdErr != nil {
		panic("Failed to marshal metadata struct")
	}

	return ProposedEvent{
		EventId:      eventId,
		EventType:    StreamMetadataType,
		ContentType:  ContentTypeJson,
		Data:         jsonBytes,
		UserMetadata: nil,
	}
}
