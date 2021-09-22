package protoutils

// ToAppendHeader ...
//func ToAppendHeader(streamID string, streamRevision stream_revision.StreamRevision) *api.AppendReq {
//	appendReq := &api.AppendReq{
//		Content: &api.AppendReq_Options_{
//			Options: &api.AppendReq_Options{},
//		},
//	}
//	appendReq.GetOptions().StreamIdentifier = &shared.StreamIdentifier{
//		StreamName: []byte(streamID),
//	}
//	switch streamRevision {
//	case stream_revision.StreamRevisionAny:
//		appendReq.GetOptions().ExpectedStreamRevision = &api.AppendReq_Options_Any{
//			Any: &shared.Empty{},
//		}
//	case stream_revision.StreamRevisionNoStream:
//		appendReq.GetOptions().ExpectedStreamRevision = &api.AppendReq_Options_NoStream{
//			NoStream: &shared.Empty{},
//		}
//	case stream_revision.StreamRevisionStreamExists:
//		appendReq.GetOptions().ExpectedStreamRevision = &api.AppendReq_Options_StreamExists{
//			StreamExists: &shared.Empty{},
//		}
//	default:
//		appendReq.GetOptions().ExpectedStreamRevision = &api.AppendReq_Options_Revision{
//			Revision: uint64(streamRevision),
//		}
//	}
//	return appendReq
//}
//
//// ToProposedMessage ...
//func ToProposedMessage(event messages.ProposedEvent) *api.AppendReq_ProposedMessage {
//	metadata := map[string]string{}
//	metadata[system_metadata.SystemMetadataKeysContentType] = event.ContentType
//	metadata[system_metadata.SystemMetadataKeysType] = event.EventType
//	return &api.AppendReq_ProposedMessage{
//		Id: &shared.UUID{
//			Value: &shared.UUID_String_{
//				String_: event.EventID.String(),
//			},
//		},
//		Data:           event.Data,
//		CustomMetadata: event.UserMetadata,
//		Metadata:       metadata,
//	}
//}
//
//// toReadDirectionFromDirection ...
//func toReadDirectionFromDirection(dir direction.Direction) api.ReadReq_Options_ReadDirection {
//	var readDirection api.ReadReq_Options_ReadDirection
//	switch dir {
//	case direction.Forwards:
//		readDirection = api.ReadReq_Options_Forwards
//	case direction.Backwards:
//		readDirection = api.ReadReq_Options_Backwards
//	}
//	return readDirection
//}
//
//// toAllReadOptionsFromPosition ...
//func toAllReadOptionsFromPosition(position stream_position.AllStreamPosition) *api.ReadReq_Options_All {
//	all := &allStream{
//		options: &api.ReadReq_Options_AllOptions{},
//	}
//
//	position.AcceptAllVisitor(all)
//
//	return &api.ReadReq_Options_All{
//		All: all.options,
//	}
//}
//
//type allStream struct {
//	options *api.ReadReq_Options_AllOptions
//}
//
//func (all *allStream) VisitStart() {
//	all.options.AllOption = &api.ReadReq_Options_AllOptions_Start{
//		Start: &shared.Empty{},
//	}
//}
//
//func (all *allStream) VisitEnd() {
//	all.options.AllOption = &api.ReadReq_Options_AllOptions_End{
//		End: &shared.Empty{},
//	}
//}
//
//func (all *allStream) VisitPosition(value position.Position) {
//	all.options.AllOption = &api.ReadReq_Options_AllOptions_Position{
//		Position: &api.ReadReq_Options_Position{
//			PreparePosition: value.Prepare,
//			CommitPosition:  value.Commit,
//		},
//	}
//}
//
//func toReadStreamOptionsFromStreamAndStreamRevision(streamID string, streamPosition stream_position.StreamPosition) *api.ReadReq_Options_Stream {
//	options := &api.ReadReq_Options_StreamOptions{
//		StreamIdentifier: &shared.StreamIdentifier{
//			StreamName: []byte(streamID),
//		},
//	}
//
//	regular := &regularStream{
//		options: options,
//	}
//
//	streamPosition.AcceptRegularVisitor(regular)
//
//	return &api.ReadReq_Options_Stream{
//		Stream: regular.options,
//	}
//}
//
//type regularStream struct {
//	options *api.ReadReq_Options_StreamOptions
//}
//
//func (reg *regularStream) VisitRevision(value uint64) {
//	reg.options.RevisionOption = &api.ReadReq_Options_StreamOptions_Revision{
//		Revision: value,
//	}
//}
//
//func (reg *regularStream) VisitStart() {
//	reg.options.RevisionOption = &api.ReadReq_Options_StreamOptions_Start{
//		Start: &shared.Empty{},
//	}
//}
//
//func (reg *regularStream) VisitEnd() {
//	reg.options.RevisionOption = &api.ReadReq_Options_StreamOptions_End{
//		End: &shared.Empty{},
//	}
//}
//
//// toFilterOptions ...
//func toFilterOptions(options filtering.SubscriptionFilterOptions) (*api.ReadReq_Options_FilterOptions, error) {
//	if len(options.SubscriptionFilter.Prefixes) == 0 && len(options.SubscriptionFilter.Regex) == 0 {
//		return nil, fmt.Errorf("The subscription filter requires a set of prefixes or a regex")
//	}
//	if len(options.SubscriptionFilter.Prefixes) > 0 && len(options.SubscriptionFilter.Regex) > 0 {
//		return nil, fmt.Errorf("The subscription filter may only contain a regex or a set of prefixes, but not both.")
//	}
//	filterOptions := api.ReadReq_Options_FilterOptions{
//		CheckpointIntervalMultiplier: uint32(options.CheckpointInterval),
//	}
//	if options.SubscriptionFilter.FilterType == filtering.EventFilter {
//		filterOptions.Filter = &api.ReadReq_Options_FilterOptions_EventType{
//			EventType: &api.ReadReq_Options_FilterOptions_Expression{
//				Prefix: options.SubscriptionFilter.Prefixes,
//				Regex:  options.SubscriptionFilter.Regex,
//			},
//		}
//	} else {
//		filterOptions.Filter = &api.ReadReq_Options_FilterOptions_StreamIdentifier{
//			StreamIdentifier: &api.ReadReq_Options_FilterOptions_Expression{
//				Prefix: options.SubscriptionFilter.Prefixes,
//				Regex:  options.SubscriptionFilter.Regex,
//			},
//		}
//	}
//	if options.MaxSearchWindow == filtering.NoMaxSearchWindow {
//		filterOptions.Window = &api.ReadReq_Options_FilterOptions_Count{
//			Count: &shared.Empty{},
//		}
//	} else {
//		filterOptions.Window = &api.ReadReq_Options_FilterOptions_Max{
//			Max: uint32(options.MaxSearchWindow),
//		}
//	}
//	return &filterOptions, nil
//}
//
//func ToDeleteRequest(streamID string, streamRevision streamrevision.StreamRevision) *api.DeleteReq {
//	deleteReq := &api.DeleteReq{
//		Options: &api.DeleteReq_Options{
//			StreamIdentifier: &shared.StreamIdentifier{
//				StreamName: []byte(streamID),
//			},
//		},
//	}
//	switch streamRevision {
//	case stream_revision.StreamRevisionAny:
//		deleteReq.GetOptions().ExpectedStreamRevision = &api.DeleteReq_Options_Any{
//			Any: &shared.Empty{},
//		}
//	case stream_revision.StreamRevisionNoStream:
//		deleteReq.GetOptions().ExpectedStreamRevision = &api.DeleteReq_Options_NoStream{
//			NoStream: &shared.Empty{},
//		}
//	case stream_revision.StreamRevisionStreamExists:
//		deleteReq.GetOptions().ExpectedStreamRevision = &api.DeleteReq_Options_StreamExists{
//			StreamExists: &shared.Empty{},
//		}
//	default:
//		deleteReq.GetOptions().ExpectedStreamRevision = &api.DeleteReq_Options_Revision{
//			Revision: uint64(streamRevision),
//		}
//	}
//	return deleteReq
//}
//
//func ToTombstoneRequest(streamID string, streamRevision streamrevision.StreamRevision) *api.TombstoneReq {
//	tombstoneReq := &api.TombstoneReq{
//		Options: &api.TombstoneReq_Options{
//			StreamIdentifier: &shared.StreamIdentifier{
//				StreamName: []byte(streamID),
//			},
//		},
//	}
//	switch streamRevision {
//	case stream_revision.StreamRevisionAny:
//		tombstoneReq.GetOptions().ExpectedStreamRevision = &api.TombstoneReq_Options_Any{
//			Any: &shared.Empty{},
//		}
//	case stream_revision.StreamRevisionNoStream:
//		tombstoneReq.GetOptions().ExpectedStreamRevision = &api.TombstoneReq_Options_NoStream{
//			NoStream: &shared.Empty{},
//		}
//	case stream_revision.StreamRevisionStreamExists:
//		tombstoneReq.GetOptions().ExpectedStreamRevision = &api.TombstoneReq_Options_StreamExists{
//			StreamExists: &shared.Empty{},
//		}
//	default:
//		tombstoneReq.GetOptions().ExpectedStreamRevision = &api.TombstoneReq_Options_Revision{
//			Revision: uint64(streamRevision),
//		}
//	}
//	return tombstoneReq
//}
//
//func ToReadStreamRequest(streamID string, direction direction.Direction, from stream_position.StreamPosition, count uint64, resolveLinks bool) *api.ReadReq {
//	return &api.ReadReq{
//		Options: &api.ReadReq_Options{
//			CountOption: &api.ReadReq_Options_Count{
//				Count: count,
//			},
//			FilterOption: &api.ReadReq_Options_NoFilter{
//				NoFilter: nil,
//			},
//			ReadDirection: toReadDirectionFromDirection(direction),
//			ResolveLinks:  resolveLinks,
//			StreamOption:  toReadStreamOptionsFromStreamAndStreamRevision(streamID, from),
//			UuidOption: &api.ReadReq_Options_UUIDOption{
//				Content: &api.ReadReq_Options_UUIDOption_String_{
//					String_: nil,
//				},
//			},
//		},
//	}
//}
//
//func ToReadAllRequest(direction direction.Direction, from stream_position.AllStreamPosition, count uint64, resolveLinks bool) *api.ReadReq {
//	return &api.ReadReq{
//		Options: &api.ReadReq_Options{
//			CountOption: &api.ReadReq_Options_Count{
//				Count: count,
//			},
//			FilterOption: &api.ReadReq_Options_NoFilter{
//				NoFilter: nil,
//			},
//			ReadDirection: toReadDirectionFromDirection(direction),
//			ResolveLinks:  resolveLinks,
//			StreamOption:  toAllReadOptionsFromPosition(from),
//			UuidOption: &api.ReadReq_Options_UUIDOption{
//				Content: &api.ReadReq_Options_UUIDOption_String_{
//					String_: nil,
//				},
//			},
//		},
//	}
//}
//
//func ToStreamSubscriptionRequest(streamID string, from stream_position.StreamPosition, resolveLinks bool, filterOptions *filtering.SubscriptionFilterOptions) (*api.ReadReq, error) {
//	readReq := &api.ReadReq{
//		Options: &api.ReadReq_Options{
//			CountOption: &api.ReadReq_Options_Subscription{
//				Subscription: &api.ReadReq_Options_SubscriptionOptions{},
//			},
//			FilterOption: &api.ReadReq_Options_NoFilter{
//				NoFilter: &shared.Empty{},
//			},
//			ReadDirection: toReadDirectionFromDirection(direction.Forwards),
//			ResolveLinks:  resolveLinks,
//			StreamOption:  toReadStreamOptionsFromStreamAndStreamRevision(streamID, from),
//			UuidOption: &api.ReadReq_Options_UUIDOption{
//				Content: &api.ReadReq_Options_UUIDOption_String_{
//					String_: nil,
//				},
//			},
//		},
//	}
//	if filterOptions != nil {
//		options, err := toFilterOptions(*filterOptions)
//		if err != nil {
//			return nil, fmt.Errorf("Failed to construct subscription request. Reason: %v", err)
//		}
//		readReq.Options.FilterOption = &api.ReadReq_Options_Filter{
//			Filter: options,
//		}
//	}
//	return readReq, nil
//}
//
//func ToAllSubscriptionRequest(from stream_position.AllStreamPosition, resolveLinks bool, filterOptions *filtering.SubscriptionFilterOptions) (*api.ReadReq, error) {
//	readReq := &api.ReadReq{
//		Options: &api.ReadReq_Options{
//			CountOption: &api.ReadReq_Options_Subscription{
//				Subscription: &api.ReadReq_Options_SubscriptionOptions{},
//			},
//			FilterOption: &api.ReadReq_Options_NoFilter{
//				NoFilter: &shared.Empty{},
//			},
//			ReadDirection: toReadDirectionFromDirection(direction.Forwards),
//			ResolveLinks:  resolveLinks,
//			StreamOption:  toAllReadOptionsFromPosition(from),
//			UuidOption: &api.ReadReq_Options_UUIDOption{
//				Content: &api.ReadReq_Options_UUIDOption_String_{
//					String_: nil,
//				},
//			},
//		},
//	}
//	if filterOptions != nil {
//		options, err := toFilterOptions(*filterOptions)
//		if err != nil {
//			return nil, fmt.Errorf("Failed to construct subscription request. Reason: %v", err)
//		}
//		readReq.Options.FilterOption = &api.ReadReq_Options_Filter{
//			Filter: options,
//		}
//	}
//	return readReq, nil
//}
//
//// EventIDFromProto ...
//func EventIDFromProto(recordedEvent *api.ReadResp_ReadEvent_RecordedEvent) uuid.UUID {
//	id := recordedEvent.GetId()
//	idString := id.GetString_()
//	return uuid.FromStringOrNil(idString)
//}
//
//// CreatedFromProto ...
//func CreatedFromProto(recordedEvent *api.ReadResp_ReadEvent_RecordedEvent) time.Time {
//	timeSinceEpoch, err := strconv.ParseInt(recordedEvent.Metadata[system_metadata.SystemMetadataKeysCreated], 10, 64)
//	if err != nil {
//		log.Fatalf("Failed to parse created date as int from %+v", recordedEvent.Metadata[system_metadata.SystemMetadataKeysCreated])
//	}
//	// The metadata contains the number of .NET "ticks" (100ns increments) since the UNIX epoch
//	return time.Unix(0, timeSinceEpoch*100).UTC()
//}
//
//func PositionFromProto(recordedEvent *api.ReadResp_ReadEvent_RecordedEvent) position.Position {
//	return position.Position{Commit: recordedEvent.GetCommitPosition(), Prepare: recordedEvent.GetPreparePosition()}
//}
//
//func DeletePositionFromProto(deleteResponse *api.DeleteResp) position.Position {
//	return position.Position{
//		Commit:  deleteResponse.GetPosition().CommitPosition,
//		Prepare: deleteResponse.GetPosition().PreparePosition,
//	}
//}
//
//func TombstonePositionFromProto(tombstoneResponse *api.TombstoneResp) position.Position {
//	return position.Position{
//		Commit:  tombstoneResponse.GetPosition().CommitPosition,
//		Prepare: tombstoneResponse.GetPosition().PreparePosition,
//	}
//}
//
//// GetContentTypeFromProto ...
//func GetContentTypeFromProto(recordedEvent *api.ReadResp_ReadEvent_RecordedEvent) string {
//	return recordedEvent.Metadata[system_metadata.SystemMetadataKeysContentType]
//}
//
//// RecordedEventFromProto
//func RecordedEventFromProto(result *api.ReadResp_ReadEvent) messages.RecordedEvent {
//	recordedEvent := result.GetEvent()
//	return GetRecordedEventFromProto(recordedEvent)
//}
//func GetRecordedEventFromProto(recordedEvent *api.ReadResp_ReadEvent_RecordedEvent) messages.RecordedEvent {
//	streamIdentifier := recordedEvent.GetStreamIdentifier()
//	return messages.RecordedEvent{
//		EventID:        EventIDFromProto(recordedEvent),
//		EventType:      recordedEvent.Metadata[system_metadata.SystemMetadataKeysType],
//		ContentType:    GetContentTypeFromProto(recordedEvent),
//		StreamID:       string(streamIdentifier.StreamName),
//		EventNumber:    recordedEvent.GetStreamRevision(),
//		CreatedDate:    CreatedFromProto(recordedEvent),
//		Position:       PositionFromProto(recordedEvent),
//		Data:           recordedEvent.GetData(),
//		SystemMetadata: recordedEvent.GetMetadata(),
//		UserMetadata:   recordedEvent.GetCustomMetadata(),
//	}
//}
//
//func GetResolvedEventFromProto(result *api.ReadResp_ReadEvent) messages.ResolvedEvent {
//	positionWire := result.GetPosition()
//	linkWire := result.GetLink()
//	eventWire := result.GetEvent()
//
//	var event *messages.RecordedEvent = nil
//	var link *messages.RecordedEvent = nil
//	var commit *uint64
//
//	if positionWire != nil {
//		switch value := positionWire.(type) {
//		case *api.ReadResp_ReadEvent_CommitPosition:
//			{
//				commit = &value.CommitPosition
//			}
//		case *api.ReadResp_ReadEvent_NoPosition:
//			{
//				commit = nil
//			}
//		}
//	}
//
//	if eventWire != nil {
//		recordedEvent := GetRecordedEventFromProto(eventWire)
//		event = &recordedEvent
//	}
//
//	if linkWire != nil {
//		recordedEvent := GetRecordedEventFromProto(linkWire)
//		link = &recordedEvent
//	}
//
//	return messages.ResolvedEvent{
//		Event:  event,
//		Link:   link,
//		Commit: commit,
//	}
//}
