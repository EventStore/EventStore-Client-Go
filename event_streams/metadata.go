package event_streams

import (
	"encoding/json"
	"time"

	"github.com/gofrs/uuid"
)

type StreamMetadataResult interface {
	IsNone() bool
	GetStreamMetadata() StreamMetadata
}

type StreamMetadataNone struct{}

func (metadataNone StreamMetadataNone) IsNone() bool {
	return true
}

func (metadataNone StreamMetadataNone) GetStreamMetadata() StreamMetadata {
	return StreamMetadata{}
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

func NewStreamMetadataResultImpl(streamId string, event ReadResponseEvent) StreamMetadataResultImpl {
	var metaData StreamMetadata
	if err := json.Unmarshal(event.Event.Data, &metaData); err != nil {
		panic(err)
	}

	revision, isCommitPosition := event.GetCommitPosition()

	if !isCommitPosition {
		panic("No position received")
	}

	return StreamMetadataResultImpl{
		StreamId:           streamId,
		StreamMetadata:     metaData,
		MetaStreamRevision: revision,
	}
}

const StreamMetadataType = "$metadata"

type StreamMetadata struct {
	MaxAge         *time.Duration
	TruncateBefore *uint64
	CacheControl   *time.Duration
	Acl            *StreamAcl
	MaxCount       *int
	CustomMetadata []byte
}

func NewStreamMetadata(
	MaxAge *time.Duration,
	TruncateBefore *uint64,
	CacheControl *time.Duration,
	Acl *StreamAcl,
	MaxCount *int,
	CustomMetadata []byte) StreamMetadata {
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
