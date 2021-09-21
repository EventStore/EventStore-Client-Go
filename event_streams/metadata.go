package event_streams

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/gofrs/uuid"
)

type StreamMetadataResult interface {
	IsNone() bool
	GetStreamMetadata() StreamMetadata
	GetMetaStreamRevision() uint64
}

type StreamMetadataNone struct{}

func (metadataNone StreamMetadataNone) IsNone() bool {
	return true
}

func (metadataNone StreamMetadataNone) GetStreamMetadata() StreamMetadata {
	return StreamMetadata{}
}

func (metadataNone StreamMetadataNone) GetMetaStreamRevision() uint64 {
	return 0
}

type StreamMetadataResultImpl struct {
	StreamId           string
	StreamMetadata     StreamMetadata
	MetaStreamRevision uint64
}

func (result StreamMetadataResultImpl) IsNone() bool {
	return false
}

func (result StreamMetadataResultImpl) GetStreamMetadata() StreamMetadata {
	return result.StreamMetadata
}

func (result StreamMetadataResultImpl) GetMetaStreamRevision() uint64 {
	return result.MetaStreamRevision
}

func NewStreamMetadataResultImpl(streamId string, event ReadResponseEvent) StreamMetadataResultImpl {
	var metaData StreamMetadata
	if err := json.Unmarshal(event.Event.Data, &metaData); err != nil {
		panic(err)
	}

	return StreamMetadataResultImpl{
		StreamId:           streamId,
		StreamMetadata:     metaData,
		MetaStreamRevision: event.Event.StreamRevision,
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

type CustomMetadataType map[string]interface{}

type StreamMetadata struct {
	MaxAge         *time.Duration `json:"$maxAge"`
	TruncateBefore *uint64        `json:"$tb"`
	CacheControl   *time.Duration `json:"$cacheControl"`
	Acl            *StreamAcl     `json:"$acl"`
	MaxCount       *int           `json:"$maxCount"`
	CustomMetadata CustomMetadataType
}

func (b StreamMetadata) MarshalJSON() ([]byte, error) {
	dat := map[string]interface{}{}

	for key, value := range b.CustomMetadata {
		if !isCustomMetaValueAllowed(value) {
			return nil, errors.New(fmt.Sprintf("custom meta data value at key=%s is not allowed", key))
		}
		dat[key] = value
	}

	dat[maxAgeJsonProperty] = b.MaxAge
	dat[truncateBeforeJsonProperty] = b.TruncateBefore
	dat[cacheControlJsonProperty] = b.CacheControl
	dat[maxCountJsonProperty] = b.MaxCount
	dat[aclJsonProperty] = b.Acl

	return json.Marshal(dat)
}

func (b *StreamMetadata) UnmarshalJSON(data []byte) error {
	var rawDataMap map[string]*json.RawMessage
	if err := json.Unmarshal(data, &rawDataMap); err != nil {
		panic(err)
	}

	if rawDataMap[maxAgeJsonProperty] != nil {
		var temp time.Duration

		stdErr := json.Unmarshal(*rawDataMap[maxAgeJsonProperty], &temp)
		if stdErr != nil {
			return stdErr
		}

		b.MaxAge = &temp
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
		var temp time.Duration
		stdErr := json.Unmarshal(*rawDataMap[cacheControlJsonProperty], &temp)
		if stdErr != nil {
			return stdErr
		}

		b.CacheControl = &temp
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

func NewStreamMetadata(
	MaxAge *time.Duration,
	TruncateBefore *uint64,
	CacheControl *time.Duration,
	Acl *StreamAcl,
	MaxCount *int,
	CustomMetadata CustomMetadataType) StreamMetadata {
	if MaxAge != nil && *MaxAge <= 0 {
		panic("Stream Metadata MaxAge is <= 0")
	}

	if CacheControl != nil && *CacheControl <= 0 {
		panic("Stream Metadata Cache Control is <= 0")
	}

	if MaxCount != nil && *MaxCount <= 0 {
		panic("Stream Metadata Max Count is <= 0")
	}

	return StreamMetadata{
		MaxAge:         MaxAge,
		TruncateBefore: TruncateBefore,
		CacheControl:   CacheControl,
		Acl:            Acl,
		MaxCount:       MaxCount,
		CustomMetadata: CustomMetadata,
	}
}

func GetMetaStreamOf(streamName string) string {
	return "$$" + streamName
}

func NewMetadataEvent(metadata StreamMetadata) ProposedEvent {
	eventId, _ := uuid.NewV4()

	jsonBytes, stdErr := json.Marshal(metadata)

	if stdErr != nil {
		panic("Failed to marshal metadata struct")
	}

	return ProposedEvent{
		EventID:      eventId,
		EventType:    StreamMetadataType,
		ContentType:  ContentTypeJson,
		Data:         jsonBytes,
		UserMetadata: nil,
	}
}
