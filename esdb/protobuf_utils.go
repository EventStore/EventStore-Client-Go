package esdb

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/EventStore/EventStore-Client-Go/v2/protos/persistent"
	"github.com/EventStore/EventStore-Client-Go/v2/protos/shared"
	api "github.com/EventStore/EventStore-Client-Go/v2/protos/streams"
	"github.com/gofrs/uuid"
)

type SubscriptionFilterOptions struct {
	MaxSearchWindow    int
	CheckpointInterval int
	SubscriptionFilter *SubscriptionFilter
}

// systemMetadataKeysType ...
const systemMetadataKeysType = "type"

// SystemMetadataKeysIsJSON ...
const systemMetadataKeysContentType = "content-type"

// systemMetadataKeysCreated ...
const systemMetadataKeysCreated = "created"

// toAppendHeader ...
func toAppendHeader(streamID string, streamRevision ExpectedRevision) *api.AppendReq {
	appendReq := &api.AppendReq{
		Content: &api.AppendReq_Options_{
			Options: &api.AppendReq_Options{},
		},
	}

	appendReq.GetOptions().StreamIdentifier = &shared.StreamIdentifier{
		StreamName: []byte(streamID),
	}

	switch value := streamRevision.(type) {
	case Any:
		appendReq.GetOptions().ExpectedStreamRevision = &api.AppendReq_Options_Any{
			Any: &shared.Empty{},
		}
	case NoStream:
		appendReq.GetOptions().ExpectedStreamRevision = &api.AppendReq_Options_NoStream{
			NoStream: &shared.Empty{},
		}
	case StreamExists:
		appendReq.GetOptions().ExpectedStreamRevision = &api.AppendReq_Options_StreamExists{
			StreamExists: &shared.Empty{},
		}
	case StreamRevision:
		appendReq.GetOptions().ExpectedStreamRevision = &api.AppendReq_Options_Revision{
			Revision: value.Value,
		}
	}

	return appendReq
}

// toProposedMessage ...
func toProposedMessage(event EventData) *api.AppendReq_ProposedMessage {
	contentType := "application/octet-stream"
	if event.ContentType == ContentTypeJson {
		contentType = "application/json"
	}

	metadata := make(map[string]string)
	metadata[systemMetadataKeysContentType] = contentType
	metadata[systemMetadataKeysType] = event.EventType
	eventId := event.EventID

	if event.Data == nil {
		event.Data = []byte{}
	}

	if event.Metadata == nil {
		event.Metadata = []byte{}
	}

	if eventId == uuid.Nil {
		eventId = uuid.Must(uuid.NewV4())
	}

	return &api.AppendReq_ProposedMessage{
		Id: &shared.UUID{
			Value: &shared.UUID_String_{
				String_: eventId.String(),
			},
		},
		Data:           event.Data,
		CustomMetadata: event.Metadata,
		Metadata:       metadata,
	}
}

// toReadDirectionFromDirection ...
func toReadDirectionFromDirection(dir Direction) api.ReadReq_Options_ReadDirection {
	var readDirection api.ReadReq_Options_ReadDirection
	switch dir {
	case Forwards:
		readDirection = api.ReadReq_Options_Forwards
	case Backwards:
		readDirection = api.ReadReq_Options_Backwards
	}
	return readDirection
}

// toAllReadOptionsFromPosition ...
func toAllReadOptionsFromPosition(position AllPosition) *api.ReadReq_Options_All {
	options := &api.ReadReq_Options_AllOptions{}

	switch value := position.(type) {
	case Start:
		options.AllOption = &api.ReadReq_Options_AllOptions_Start{
			Start: &shared.Empty{},
		}
	case End:
		options.AllOption = &api.ReadReq_Options_AllOptions_End{
			End: &shared.Empty{},
		}
	case Position:
		options.AllOption = &api.ReadReq_Options_AllOptions_Position{
			Position: &api.ReadReq_Options_Position{
				PreparePosition: value.Prepare,
				CommitPosition:  value.Commit,
			},
		}
	}

	return &api.ReadReq_Options_All{
		All: options,
	}
}

func toReadStreamOptionsFromStreamAndStreamRevision(streamID string, streamPosition StreamPosition) *api.ReadReq_Options_Stream {
	options := &api.ReadReq_Options_StreamOptions{
		StreamIdentifier: &shared.StreamIdentifier{
			StreamName: []byte(streamID),
		},
	}

	switch value := streamPosition.(type) {
	case Start:
		options.RevisionOption = &api.ReadReq_Options_StreamOptions_Start{
			Start: &shared.Empty{},
		}
	case End:
		options.RevisionOption = &api.ReadReq_Options_StreamOptions_End{
			End: &shared.Empty{},
		}
	case StreamRevision:
		options.RevisionOption = &api.ReadReq_Options_StreamOptions_Revision{
			Revision: value.Value,
		}
	}

	return &api.ReadReq_Options_Stream{
		Stream: options,
	}
}

// toFilterOptions ...
func toFilterOptions(options *SubscriptionFilterOptions) (*api.ReadReq_Options_FilterOptions, error) {
	if len(options.SubscriptionFilter.Prefixes) == 0 && len(options.SubscriptionFilter.Regex) == 0 {
		return nil, fmt.Errorf("the subscription filter requires a set of prefixes or a regex")
	}
	if len(options.SubscriptionFilter.Prefixes) > 0 && len(options.SubscriptionFilter.Regex) > 0 {
		return nil, fmt.Errorf("the subscription filter may only contain a regex or a set of prefixes, but not both")
	}
	filterOptions := api.ReadReq_Options_FilterOptions{
		CheckpointIntervalMultiplier: uint32(options.CheckpointInterval),
	}
	if options.SubscriptionFilter.Type == EventFilterType {
		filterOptions.Filter = &api.ReadReq_Options_FilterOptions_EventType{
			EventType: &api.ReadReq_Options_FilterOptions_Expression{
				Prefix: options.SubscriptionFilter.Prefixes,
				Regex:  options.SubscriptionFilter.Regex,
			},
		}
	} else {
		filterOptions.Filter = &api.ReadReq_Options_FilterOptions_StreamIdentifier{
			StreamIdentifier: &api.ReadReq_Options_FilterOptions_Expression{
				Prefix: options.SubscriptionFilter.Prefixes,
				Regex:  options.SubscriptionFilter.Regex,
			},
		}
	}
	if options.MaxSearchWindow == NoMaxSearchWindow {
		filterOptions.Window = &api.ReadReq_Options_FilterOptions_Count{
			Count: &shared.Empty{},
		}
	} else {
		filterOptions.Window = &api.ReadReq_Options_FilterOptions_Max{
			Max: uint32(options.MaxSearchWindow),
		}
	}
	return &filterOptions, nil
}

func toDeleteRequest(streamID string, streamRevision ExpectedRevision) *api.DeleteReq {
	deleteReq := &api.DeleteReq{
		Options: &api.DeleteReq_Options{
			StreamIdentifier: &shared.StreamIdentifier{
				StreamName: []byte(streamID),
			},
		},
	}

	switch value := streamRevision.(type) {
	case Any:
		deleteReq.GetOptions().ExpectedStreamRevision = &api.DeleteReq_Options_Any{
			Any: &shared.Empty{},
		}
	case StreamExists:
		deleteReq.GetOptions().ExpectedStreamRevision = &api.DeleteReq_Options_StreamExists{
			StreamExists: &shared.Empty{},
		}
	case NoStream:
		deleteReq.GetOptions().ExpectedStreamRevision = &api.DeleteReq_Options_NoStream{
			NoStream: &shared.Empty{},
		}
	case StreamRevision:
		deleteReq.GetOptions().ExpectedStreamRevision = &api.DeleteReq_Options_Revision{
			Revision: value.Value,
		}
	}

	return deleteReq
}

func toTombstoneRequest(streamID string, streamRevision ExpectedRevision) *api.TombstoneReq {
	tombstoneReq := &api.TombstoneReq{
		Options: &api.TombstoneReq_Options{
			StreamIdentifier: &shared.StreamIdentifier{
				StreamName: []byte(streamID),
			},
		},
	}

	switch value := streamRevision.(type) {
	case Any:
		tombstoneReq.GetOptions().ExpectedStreamRevision = &api.TombstoneReq_Options_Any{
			Any: &shared.Empty{},
		}
	case StreamExists:
		tombstoneReq.GetOptions().ExpectedStreamRevision = &api.TombstoneReq_Options_StreamExists{
			StreamExists: &shared.Empty{},
		}
	case NoStream:
		tombstoneReq.GetOptions().ExpectedStreamRevision = &api.TombstoneReq_Options_NoStream{
			NoStream: &shared.Empty{},
		}
	case StreamRevision:
		tombstoneReq.GetOptions().ExpectedStreamRevision = &api.TombstoneReq_Options_Revision{
			Revision: value.Value,
		}
	}

	return tombstoneReq
}

func toReadStreamRequest(streamID string, direction Direction, from StreamPosition, count uint64, resolveLinks bool) *api.ReadReq {
	return &api.ReadReq{
		Options: &api.ReadReq_Options{
			CountOption: &api.ReadReq_Options_Count{
				Count: count,
			},
			FilterOption: &api.ReadReq_Options_NoFilter{
				NoFilter: nil,
			},
			ReadDirection: toReadDirectionFromDirection(direction),
			ResolveLinks:  resolveLinks,
			StreamOption:  toReadStreamOptionsFromStreamAndStreamRevision(streamID, from),
			UuidOption: &api.ReadReq_Options_UUIDOption{
				Content: &api.ReadReq_Options_UUIDOption_String_{
					String_: nil,
				},
			},
		},
	}
}

func toReadAllRequest(direction Direction, from AllPosition, count uint64, resolveLinks bool) *api.ReadReq {
	return &api.ReadReq{
		Options: &api.ReadReq_Options{
			CountOption: &api.ReadReq_Options_Count{
				Count: count,
			},
			FilterOption: &api.ReadReq_Options_NoFilter{
				NoFilter: nil,
			},
			ReadDirection: toReadDirectionFromDirection(direction),
			ResolveLinks:  resolveLinks,
			StreamOption:  toAllReadOptionsFromPosition(from),
			UuidOption: &api.ReadReq_Options_UUIDOption{
				Content: &api.ReadReq_Options_UUIDOption_String_{
					String_: nil,
				},
			},
		},
	}
}

func toStreamSubscriptionRequest(streamID string, from StreamPosition, resolveLinks bool, filterOptions *SubscriptionFilterOptions) (*api.ReadReq, error) {
	readReq := &api.ReadReq{
		Options: &api.ReadReq_Options{
			CountOption: &api.ReadReq_Options_Subscription{
				Subscription: &api.ReadReq_Options_SubscriptionOptions{},
			},
			FilterOption: &api.ReadReq_Options_NoFilter{
				NoFilter: &shared.Empty{},
			},
			ReadDirection: toReadDirectionFromDirection(Forwards),
			ResolveLinks:  resolveLinks,
			StreamOption:  toReadStreamOptionsFromStreamAndStreamRevision(streamID, from),
			UuidOption: &api.ReadReq_Options_UUIDOption{
				Content: &api.ReadReq_Options_UUIDOption_String_{
					String_: nil,
				},
			},
		},
	}
	if filterOptions != nil {
		options, err := toFilterOptions(filterOptions)
		if err != nil {
			return nil, fmt.Errorf("Failed to construct subscription request. Reason: %v", err)
		}
		readReq.Options.FilterOption = &api.ReadReq_Options_Filter{
			Filter: options,
		}
	}
	return readReq, nil
}

func toAllSubscriptionRequest(from AllPosition, resolveLinks bool, filterOptions *SubscriptionFilterOptions) (*api.ReadReq, error) {
	readReq := &api.ReadReq{
		Options: &api.ReadReq_Options{
			CountOption: &api.ReadReq_Options_Subscription{
				Subscription: &api.ReadReq_Options_SubscriptionOptions{},
			},
			FilterOption: &api.ReadReq_Options_NoFilter{
				NoFilter: &shared.Empty{},
			},
			ReadDirection: toReadDirectionFromDirection(Forwards),
			ResolveLinks:  resolveLinks,
			StreamOption:  toAllReadOptionsFromPosition(from),
			UuidOption: &api.ReadReq_Options_UUIDOption{
				Content: &api.ReadReq_Options_UUIDOption_String_{
					String_: nil,
				},
			},
		},
	}
	if filterOptions != nil {
		options, err := toFilterOptions(filterOptions)
		if err != nil {
			return nil, fmt.Errorf("Failed to construct subscription request. Reason: %v", err)
		}
		readReq.Options.FilterOption = &api.ReadReq_Options_Filter{
			Filter: options,
		}
	}
	return readReq, nil
}

// eventIDFromProto ...
func eventIDFromProto(recordedEvent *api.ReadResp_ReadEvent_RecordedEvent) uuid.UUID {
	id := recordedEvent.GetId()
	idString := id.GetString_()
	return uuid.FromStringOrNil(idString)
}

// createdFromProto ...
func createdFromProto(recordedEvent *api.ReadResp_ReadEvent_RecordedEvent) time.Time {
	timeSinceEpoch, err := strconv.ParseInt(recordedEvent.Metadata[systemMetadataKeysCreated], 10, 64)
	if err != nil {
		log.Fatalf("Failed to parse created date as int from %+v", recordedEvent.Metadata[systemMetadataKeysCreated])
	}
	// The metadata contains the number of .NET "ticks" (100ns increments) since the UNIX epoch
	return time.Unix(0, timeSinceEpoch*100).UTC()
}

func positionFromProto(recordedEvent *api.ReadResp_ReadEvent_RecordedEvent) Position {
	return Position{Commit: recordedEvent.GetCommitPosition(), Prepare: recordedEvent.GetPreparePosition()}
}

func deletePositionFromProto(deleteResponse *api.DeleteResp) Position {
	return Position{
		Commit:  deleteResponse.GetPosition().CommitPosition,
		Prepare: deleteResponse.GetPosition().PreparePosition,
	}
}

func tombstonePositionFromProto(tombstoneResponse *api.TombstoneResp) Position {
	return Position{
		Commit:  tombstoneResponse.GetPosition().CommitPosition,
		Prepare: tombstoneResponse.GetPosition().PreparePosition,
	}
}

// getContentTypeFromProto ...
func getContentTypeFromProto(recordedEvent *api.ReadResp_ReadEvent_RecordedEvent) string {
	return recordedEvent.Metadata[systemMetadataKeysContentType]
}

// recordedEventFromProto
func recordedEventFromProto(result *api.ReadResp_ReadEvent) RecordedEvent {
	recordedEvent := result.GetEvent()
	return getRecordedEventFromProto(recordedEvent)
}
func getRecordedEventFromProto(recordedEvent *api.ReadResp_ReadEvent_RecordedEvent) RecordedEvent {
	streamIdentifier := recordedEvent.GetStreamIdentifier()
	return RecordedEvent{
		EventID:        eventIDFromProto(recordedEvent),
		EventType:      recordedEvent.Metadata[systemMetadataKeysType],
		ContentType:    getContentTypeFromProto(recordedEvent),
		StreamID:       string(streamIdentifier.StreamName),
		EventNumber:    recordedEvent.GetStreamRevision(),
		CreatedDate:    createdFromProto(recordedEvent),
		Position:       positionFromProto(recordedEvent),
		Data:           recordedEvent.GetData(),
		SystemMetadata: recordedEvent.GetMetadata(),
		UserMetadata:   recordedEvent.GetCustomMetadata(),
	}
}

func getResolvedEventFromProto(result *api.ReadResp_ReadEvent) ResolvedEvent {
	positionWire := result.GetPosition()
	linkWire := result.GetLink()
	eventWire := result.GetEvent()

	var event *RecordedEvent = nil
	var link *RecordedEvent = nil
	var commit *uint64

	if positionWire != nil {
		switch value := positionWire.(type) {
		case *api.ReadResp_ReadEvent_CommitPosition:
			{
				commit = &value.CommitPosition
			}
		case *api.ReadResp_ReadEvent_NoPosition:
			{
				commit = nil
			}
		}
	}

	if eventWire != nil {
		recordedEvent := getRecordedEventFromProto(eventWire)
		event = &recordedEvent
	}

	if linkWire != nil {
		recordedEvent := getRecordedEventFromProto(linkWire)
		link = &recordedEvent
	}

	return ResolvedEvent{
		Event:  event,
		Link:   link,
		Commit: commit,
	}
}

func eventIDFromPersistentProto(recordedEvent *persistent.ReadResp_ReadEvent_RecordedEvent) uuid.UUID {
	id := recordedEvent.GetId()
	idString := id.GetString_()
	return uuid.FromStringOrNil(idString)
}

func toProtoUUID(id uuid.UUID) *shared.UUID {
	return &shared.UUID{
		Value: &shared.UUID_String_{
			String_: id.String(),
		},
	}
}

func getContentTypeFromPersistentProto(recordedEvent *persistent.ReadResp_ReadEvent_RecordedEvent) string {
	return recordedEvent.Metadata[systemMetadataKeysContentType]
}

func createdFromPersistentProto(recordedEvent *persistent.ReadResp_ReadEvent_RecordedEvent) time.Time {
	timeSinceEpoch, err := strconv.ParseInt(
		recordedEvent.Metadata[systemMetadataKeysCreated], 10, 64)
	if err != nil {
		log.Fatalf("Failed to parse created date as int from %+v",
			recordedEvent.Metadata[systemMetadataKeysCreated])
	}
	// The metadata contains the number of .NET "ticks" (100ns increments) since the UNIX epoch
	return time.Unix(0, timeSinceEpoch*100).UTC()
}

func positionFromPersistentProto(recordedEvent *persistent.ReadResp_ReadEvent_RecordedEvent) Position {
	return Position{
		Commit:  recordedEvent.GetCommitPosition(),
		Prepare: recordedEvent.GetPreparePosition(),
	}
}

func fromPersistentProtoResponse(resp *persistent.ReadResp) (*ResolvedEvent, int) {
	readEvent := resp.GetEvent()
	positionWire := readEvent.GetPosition()
	eventWire := readEvent.GetEvent()
	linkWire := readEvent.GetLink()
	countWire := readEvent.GetCount()

	var event *RecordedEvent = nil
	var link *RecordedEvent = nil
	var commit *uint64
	retryCount := 0

	if positionWire != nil {
		switch value := positionWire.(type) {
		case *persistent.ReadResp_ReadEvent_CommitPosition:
			{
				commit = &value.CommitPosition
			}
		case *persistent.ReadResp_ReadEvent_NoPosition:
			{
				commit = nil
			}
		}
	}

	if countWire != nil {
		switch value := countWire.(type) {
		case *persistent.ReadResp_ReadEvent_RetryCount:
			{
				retryCount = int(value.RetryCount)
			}
		case *persistent.ReadResp_ReadEvent_NoRetryCount:
			{
				retryCount = 0
			}
		}
	}

	if eventWire != nil {
		recordedEvent := newMessageFromPersistentProto(eventWire)
		event = &recordedEvent
	}

	if linkWire != nil {
		recordedEvent := newMessageFromPersistentProto(linkWire)
		link = &recordedEvent
	}

	return &ResolvedEvent{
		Event:  event,
		Link:   link,
		Commit: commit,
	}, retryCount
}

func newMessageFromPersistentProto(recordedEvent *persistent.ReadResp_ReadEvent_RecordedEvent) RecordedEvent {
	streamIdentifier := recordedEvent.GetStreamIdentifier()

	return RecordedEvent{
		EventID:        eventIDFromPersistentProto(recordedEvent),
		EventType:      recordedEvent.Metadata[systemMetadataKeysType],
		ContentType:    getContentTypeFromPersistentProto(recordedEvent),
		StreamID:       string(streamIdentifier.StreamName),
		EventNumber:    recordedEvent.GetStreamRevision(),
		CreatedDate:    createdFromPersistentProto(recordedEvent),
		Position:       positionFromPersistentProto(recordedEvent),
		Data:           recordedEvent.GetData(),
		SystemMetadata: recordedEvent.GetMetadata(),
		UserMetadata:   recordedEvent.GetCustomMetadata(),
	}
}

func updatePersistentRequestStreamProto(
	streamName string,
	groupName string,
	position StreamPosition,
	settings PersistentSubscriptionSettings,
) *persistent.UpdateReq {
	return &persistent.UpdateReq{
		Options: updatePersistentSubscriptionStreamConfigProto(streamName, groupName, position, settings),
	}
}

func updatePersistentRequestAllOptionsProto(
	groupName string,
	position AllPosition,
	settings PersistentSubscriptionSettings,
) *persistent.UpdateReq {
	options := updatePersistentRequestAllOptionsSettingsProto(position)

	return &persistent.UpdateReq{
		Options: &persistent.UpdateReq_Options{
			StreamOption: options,
			GroupName:    groupName,
			Settings:     updatePersistentSubscriptionSettingsProto(settings),
		},
	}
}

func updatePersistentRequestAllOptionsSettingsProto(
	position AllPosition,
) *persistent.UpdateReq_Options_All {
	options := &persistent.UpdateReq_Options_All{
		All: &persistent.UpdateReq_AllOptions{
			AllOption: nil,
		},
	}

	switch value := position.(type) {
	case Start:
		options.All.AllOption = &persistent.UpdateReq_AllOptions_Start{
			Start: &shared.Empty{},
		}
	case End:
		options.All.AllOption = &persistent.UpdateReq_AllOptions_Start{
			Start: &shared.Empty{},
		}
	case Position:
		options.All.AllOption = toUpdatePersistentRequestAllOptionsFromPosition(value)
	}

	return options
}

func updatePersistentSubscriptionStreamConfigProto(
	streamName string,
	groupName string,
	position StreamPosition,
	settings PersistentSubscriptionSettings,
) *persistent.UpdateReq_Options {
	return &persistent.UpdateReq_Options{
		StreamOption: updatePersistentSubscriptionStreamSettingsProto(streamName, groupName, position),
		// backward compatibility
		StreamIdentifier: &shared.StreamIdentifier{
			StreamName: []byte(streamName),
		},
		GroupName: groupName,
		Settings:  updatePersistentSubscriptionSettingsProto(settings),
	}
}

func updatePersistentSubscriptionStreamSettingsProto(
	streamName string,
	groupName string,
	position StreamPosition,
) *persistent.UpdateReq_Options_Stream {
	streamOption := &persistent.UpdateReq_Options_Stream{
		Stream: &persistent.UpdateReq_StreamOptions{
			StreamIdentifier: &shared.StreamIdentifier{
				StreamName: []byte(streamName),
			},
			RevisionOption: nil,
		},
	}

	switch value := position.(type) {
	case Start:
		streamOption.Stream.RevisionOption = &persistent.UpdateReq_StreamOptions_Start{
			Start: &shared.Empty{},
		}
	case End:
		streamOption.Stream.RevisionOption = &persistent.UpdateReq_StreamOptions_Start{
			Start: &shared.Empty{},
		}
	case StreamRevision:
		streamOption.Stream.RevisionOption = &persistent.UpdateReq_StreamOptions_Revision{
			Revision: value.Value,
		}
	}

	return streamOption
}

func updatePersistentSubscriptionSettingsProto(
	settings PersistentSubscriptionSettings,
) *persistent.UpdateReq_Settings {
	return &persistent.UpdateReq_Settings{
		ResolveLinks:          settings.ResolveLinkTos,
		ExtraStatistics:       settings.ExtraStatistics,
		MaxRetryCount:         settings.MaxRetryCount,
		MinCheckpointCount:    settings.CheckpointLowerBound,
		MaxCheckpointCount:    settings.CheckpointUpperBound,
		MaxSubscriberCount:    settings.MaxSubscriberCount,
		LiveBufferSize:        settings.LiveBufferSize,
		ReadBatchSize:         settings.ReadBatchSize,
		HistoryBufferSize:     settings.HistoryBufferSize,
		NamedConsumerStrategy: updatePersistentRequestConsumerStrategyProto(settings.ConsumerStrategyName),
		MessageTimeout:        updatePersistentRequestMessageTimeOutInMsProto(settings.MessageTimeout),
		CheckpointAfter:       updatePersistentRequestCheckpointAfterMsProto(settings.CheckpointAfter),
	}
}

func updatePersistentRequestConsumerStrategyProto(
	strategy ConsumerStrategy,
) persistent.UpdateReq_ConsumerStrategy {
	switch strategy {
	case ConsumerStrategyDispatchToSingle:
		return persistent.UpdateReq_DispatchToSingle
	case ConsumerStrategyPinned:
		return persistent.UpdateReq_Pinned
	// FIXME: support Pinned by correlation case ConsumerStrategy_PinnedByCorrelation:
	case ConsumerStrategyRoundRobin:
		return persistent.UpdateReq_RoundRobin
	default:
		panic(fmt.Sprintf("Could not map strategy %v to proto", strategy))
	}
}

func updatePersistentRequestMessageTimeOutInMsProto(
	timeout int32,
) *persistent.UpdateReq_Settings_MessageTimeoutMs {
	return &persistent.UpdateReq_Settings_MessageTimeoutMs{
		MessageTimeoutMs: timeout,
	}
}

func updatePersistentRequestCheckpointAfterMsProto(
	checkpointAfterMs int32,
) *persistent.UpdateReq_Settings_CheckpointAfterMs {
	return &persistent.UpdateReq_Settings_CheckpointAfterMs{
		CheckpointAfterMs: checkpointAfterMs,
	}
}

// toUpdatePersistentRequestAllOptionsFromPosition ...
func toUpdatePersistentRequestAllOptionsFromPosition(position Position) *persistent.UpdateReq_AllOptions_Position {
	return &persistent.UpdateReq_AllOptions_Position{
		Position: &persistent.UpdateReq_Position{
			PreparePosition: position.Prepare,
			CommitPosition:  position.Commit,
		},
	}
}

func createPersistentRequestProto(
	streamName string,
	groupName string,
	position StreamPosition,
	settings PersistentSubscriptionSettings,
) *persistent.CreateReq {
	return &persistent.CreateReq{
		Options: createPersistentSubscriptionStreamConfigProto(streamName, groupName, position, settings),
	}
}

func createPersistentRequestAllOptionsProto(
	groupName string,
	position AllPosition,
	settings PersistentSubscriptionSettings,
	filter *SubscriptionFilterOptions,
) (*persistent.CreateReq, error) {
	options, err := createPersistentRequestAllOptionsSettingsProto(position, filter)
	if err != nil {
		return nil, err
	}

	return &persistent.CreateReq{
		Options: &persistent.CreateReq_Options{
			StreamOption: options,
			GroupName:    groupName,
			Settings:     createPersistentSubscriptionSettingsProto(nil, settings),
		},
	}, nil
}

func createPersistentRequestAllOptionsSettingsProto(
	pos AllPosition,
	filter *SubscriptionFilterOptions,
) (*persistent.CreateReq_Options_All, error) {
	options := &persistent.CreateReq_Options_All{
		All: &persistent.CreateReq_AllOptions{
			AllOption: nil,
			FilterOption: &persistent.CreateReq_AllOptions_NoFilter{
				NoFilter: &shared.Empty{},
			},
		},
	}

	switch value := pos.(type) {
	case Start:
		options.All.AllOption = &persistent.CreateReq_AllOptions_Start{
			Start: &shared.Empty{},
		}
	case End:
		options.All.AllOption = &persistent.CreateReq_AllOptions_End{
			End: &shared.Empty{},
		}
	case Position:
		options.All.AllOption = toCreatePersistentRequestAllOptionsFromPosition(value)
	}

	if filter != nil {
		filter, err := createRequestFilterOptionsProto(filter)
		if err != nil {
			return nil, err
		}
		options.All.FilterOption = &persistent.CreateReq_AllOptions_Filter{
			Filter: filter,
		}
	}

	return options, nil
}

func createPersistentSubscriptionStreamConfigProto(
	streamName string,
	groupName string,
	position StreamPosition,
	settings PersistentSubscriptionSettings,
) *persistent.CreateReq_Options {
	return &persistent.CreateReq_Options{
		StreamOption: createPersistentSubscriptionStreamSettingsProto(streamName, position),
		// backward compatibility
		StreamIdentifier: &shared.StreamIdentifier{
			StreamName: []byte(streamName),
		},
		GroupName: groupName,
		Settings:  createPersistentSubscriptionSettingsProto(position, settings),
	}
}

func createPersistentSubscriptionStreamSettingsProto(
	streamName string,
	position StreamPosition,
) *persistent.CreateReq_Options_Stream {
	streamOption := &persistent.CreateReq_Options_Stream{
		Stream: &persistent.CreateReq_StreamOptions{
			StreamIdentifier: &shared.StreamIdentifier{
				StreamName: []byte(streamName),
			},
		},
	}

	switch value := position.(type) {
	case Start:
		streamOption.Stream.RevisionOption = &persistent.CreateReq_StreamOptions_Start{
			Start: &shared.Empty{},
		}
	case End:
		streamOption.Stream.RevisionOption = &persistent.CreateReq_StreamOptions_End{
			End: &shared.Empty{},
		}
	case StreamRevision:
		streamOption.Stream.RevisionOption = &persistent.CreateReq_StreamOptions_Revision{
			Revision: value.Value,
		}
	}

	return streamOption
}

func createPersistentSubscriptionSettingsProto(
	position StreamPosition,
	settings PersistentSubscriptionSettings,
) *persistent.CreateReq_Settings {
	var revision uint64 = 0

	// We only do this to be compatible with pre-21.* servers. On recent servers, that field is deprecated and simply
	// ignored by the server.
	if position != nil {
		switch value := position.(type) {
		case Start:
			revision = 0
			break
		case End:
			revision = ^uint64(0)
			break
		case StreamRevision:
			revision = value.Value
			break
		}
	}

	return &persistent.CreateReq_Settings{
		Revision:              revision,
		ResolveLinks:          settings.ResolveLinkTos,
		ExtraStatistics:       settings.ExtraStatistics,
		MaxRetryCount:         settings.MaxRetryCount,
		MinCheckpointCount:    settings.CheckpointLowerBound,
		MaxCheckpointCount:    settings.CheckpointUpperBound,
		MaxSubscriberCount:    settings.MaxSubscriberCount,
		LiveBufferSize:        settings.LiveBufferSize,
		ReadBatchSize:         settings.ReadBatchSize,
		HistoryBufferSize:     settings.HistoryBufferSize,
		NamedConsumerStrategy: consumerStrategyProto(settings.ConsumerStrategyName),
		MessageTimeout:        messageTimeOutInMsProto(settings.MessageTimeout),
		CheckpointAfter:       checkpointAfterMsProto(settings.CheckpointAfter),
	}
}

func consumerStrategyProto(strategy ConsumerStrategy) persistent.CreateReq_ConsumerStrategy {
	switch strategy {
	case ConsumerStrategyDispatchToSingle:
		return persistent.CreateReq_DispatchToSingle
	case ConsumerStrategyPinned:
		return persistent.CreateReq_Pinned
	case ConsumerStrategyRoundRobin:
		return persistent.CreateReq_RoundRobin
	default:
		panic(fmt.Sprintf("Could not map strategy %v to proto", strategy))
	}
}

func messageTimeOutInMsProto(timeout int32) *persistent.CreateReq_Settings_MessageTimeoutMs {
	return &persistent.CreateReq_Settings_MessageTimeoutMs{
		MessageTimeoutMs: timeout,
	}
}

func checkpointAfterMsProto(checkpointAfterMs int32) *persistent.CreateReq_Settings_CheckpointAfterMs {
	return &persistent.CreateReq_Settings_CheckpointAfterMs{
		CheckpointAfterMs: checkpointAfterMs,
	}
}

// createRequestFilterOptionsProto ...
func createRequestFilterOptionsProto(
	options *SubscriptionFilterOptions,
) (*persistent.CreateReq_AllOptions_FilterOptions, error) {
	if len(options.SubscriptionFilter.Prefixes) == 0 && len(options.SubscriptionFilter.Regex) == 0 {
		return nil, &Error{code: ErrorCodeUnknown, err: fmt.Errorf("persistent subscription to $all must provide regex or prefixes")}

	}
	if len(options.SubscriptionFilter.Prefixes) > 0 && len(options.SubscriptionFilter.Regex) > 0 {
		return nil, &Error{code: ErrorCodeUnknown, err: fmt.Errorf("persistent subscription to $all must provide regex or prefixes")}
	}
	filterOptions := persistent.CreateReq_AllOptions_FilterOptions{
		CheckpointIntervalMultiplier: uint32(options.CheckpointInterval),
	}
	if options.SubscriptionFilter.Type == EventFilterType {
		filterOptions.Filter = &persistent.CreateReq_AllOptions_FilterOptions_EventType{
			EventType: &persistent.CreateReq_AllOptions_FilterOptions_Expression{
				Prefix: options.SubscriptionFilter.Prefixes,
				Regex:  options.SubscriptionFilter.Regex,
			},
		}
	} else {
		filterOptions.Filter = &persistent.CreateReq_AllOptions_FilterOptions_StreamIdentifier{
			StreamIdentifier: &persistent.CreateReq_AllOptions_FilterOptions_Expression{
				Prefix: options.SubscriptionFilter.Prefixes,
				Regex:  options.SubscriptionFilter.Regex,
			},
		}
	}
	if options.MaxSearchWindow == NoMaxSearchWindow {
		filterOptions.Window = &persistent.CreateReq_AllOptions_FilterOptions_Count{
			Count: &shared.Empty{},
		}
	} else {
		filterOptions.Window = &persistent.CreateReq_AllOptions_FilterOptions_Max{
			Max: uint32(options.MaxSearchWindow),
		}
	}
	return &filterOptions, nil
}

// toUpdatePersistentRequestAllOptionsFromPosition ...
func toCreatePersistentRequestAllOptionsFromPosition(
	position Position,
) *persistent.CreateReq_AllOptions_Position {
	return &persistent.CreateReq_AllOptions_Position{
		Position: &persistent.CreateReq_Position{
			PreparePosition: position.Prepare,
			CommitPosition:  position.Commit,
		},
	}
}

func deletePersistentRequestStreamProto(streamName string, groupName string) *persistent.DeleteReq {
	return &persistent.DeleteReq{
		Options: &persistent.DeleteReq_Options{
			GroupName: groupName,
			StreamOption: &persistent.DeleteReq_Options_StreamIdentifier{
				StreamIdentifier: &shared.StreamIdentifier{
					StreamName: []byte(streamName),
				},
			},
		},
	}
}

func deletePersistentRequestAllOptionsProto(groupName string) *persistent.DeleteReq {
	return &persistent.DeleteReq{
		Options: &persistent.DeleteReq_Options{
			GroupName: groupName,
			StreamOption: &persistent.DeleteReq_Options_All{
				All: &shared.Empty{},
			},
		},
	}
}

func toPersistentReadRequest(
	bufferSize int32,
	groupName string,
	streamName []byte,
) *persistent.ReadReq {
	options := &persistent.ReadReq_Options{
		BufferSize: bufferSize,
		GroupName:  groupName,
		UuidOption: &persistent.ReadReq_Options_UUIDOption{
			Content: &persistent.ReadReq_Options_UUIDOption_String_{
				String_: nil,
			},
		},
	}

	if len(streamName) == 0 {
		options.StreamOption = &persistent.ReadReq_Options_All{
			All: &shared.Empty{},
		}
	} else {
		options.StreamOption = &persistent.ReadReq_Options_StreamIdentifier{
			StreamIdentifier: &shared.StreamIdentifier{
				StreamName: streamName,
			},
		}
	}

	return &persistent.ReadReq{
		Content: &persistent.ReadReq_Options_{
			Options: options,
		},
	}
}
