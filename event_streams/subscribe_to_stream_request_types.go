package event_streams

import (
	"github.com/pivonroll/EventStore-Client-Go/protos/shared"
	"github.com/pivonroll/EventStore-Client-Go/protos/streams2"
)

type SubscribeToStreamRequest struct {
	// SubscribeRequestStreamOptions
	// SubscribeRequestStreamOptionsAll
	StreamOption isSubscribeRequestStreamOptions
	Direction    SubscribeRequestDirection
	ResolveLinks bool
	// SubscribeRequestFilter
	// SubscribeRequestNoFilter
	Filter isSubscribeRequestFilterOption
}

func (this SubscribeToStreamRequest) Build() *streams2.ReadReq {
	result := &streams2.ReadReq{
		Options: &streams2.ReadReq_Options{
			ResolveLinks: this.ResolveLinks,
			FilterOption: nil,
			CountOption: &streams2.ReadReq_Options_Subscription{
				Subscription: &streams2.ReadReq_Options_SubscriptionOptions{},
			},
			UuidOption: &streams2.ReadReq_Options_UUIDOption{
				Content: &streams2.ReadReq_Options_UUIDOption_String_{
					String_: &shared.Empty{},
				},
			},
		},
	}

	if this.Direction == SubscribeRequestDirectionForward {
		result.Options.ReadDirection = streams2.ReadReq_Options_Forwards
	} else {
		result.Options.ReadDirection = streams2.ReadReq_Options_Backwards
	}

	this.buildStreamOption(result.Options)
	this.buildFilterOption(result.Options)

	return result
}

func (this SubscribeToStreamRequest) buildStreamOption(options *streams2.ReadReq_Options) {
	switch this.StreamOption.(type) {
	case SubscribeRequestStreamOptions:
		options.StreamOption = this.buildStreamOptions(
			this.StreamOption.(SubscribeRequestStreamOptions))
	case SubscribeRequestStreamOptionsAll:
		options.StreamOption = this.buildStreamOptionAll(
			this.StreamOption.(SubscribeRequestStreamOptionsAll))
	}
}

func (this SubscribeToStreamRequest) buildStreamOptionAll(all SubscribeRequestStreamOptionsAll) *streams2.ReadReq_Options_All {
	result := &streams2.ReadReq_Options_All{
		All: &streams2.ReadReq_Options_AllOptions{},
	}

	switch all.Position.(type) {
	case SubscribeRequestOptionsAllPosition:
		allPosition := all.Position.(SubscribeRequestOptionsAllPosition)
		result.All.AllOption = &streams2.ReadReq_Options_AllOptions_Position{
			Position: &streams2.ReadReq_Options_Position{
				CommitPosition:  allPosition.CommitPosition,
				PreparePosition: allPosition.PreparePosition,
			},
		}
	case SubscribeRequestOptionsAllStartPosition:
		result.All.AllOption = &streams2.ReadReq_Options_AllOptions_Start{
			Start: &shared.Empty{},
		}
	case SubscribeRequestOptionsAllEndPosition:
		result.All.AllOption = &streams2.ReadReq_Options_AllOptions_End{
			End: &shared.Empty{},
		}
	}

	return result
}

func (this SubscribeToStreamRequest) buildStreamOptions(
	streamOptions SubscribeRequestStreamOptions) *streams2.ReadReq_Options_Stream {
	result := &streams2.ReadReq_Options_Stream{
		Stream: &streams2.ReadReq_Options_StreamOptions{
			StreamIdentifier: &shared.StreamIdentifier{
				StreamName: []byte(streamOptions.StreamIdentifier),
			},
			RevisionOption: nil,
		},
	}

	switch streamOptions.Revision.(type) {
	case SubscribeRequestOptionsStreamRevision:
		streamRevision := streamOptions.Revision.(SubscribeRequestOptionsStreamRevision)
		result.Stream.RevisionOption = &streams2.ReadReq_Options_StreamOptions_Revision{
			Revision: streamRevision.Revision,
		}
	case SubscribeRequestOptionsStreamRevisionStart:
		result.Stream.RevisionOption = &streams2.ReadReq_Options_StreamOptions_Start{
			Start: &shared.Empty{},
		}
	case SubscribeRequestOptionsStreamRevisionEnd:
		result.Stream.RevisionOption = &streams2.ReadReq_Options_StreamOptions_End{
			End: &shared.Empty{},
		}
	}

	return result
}

func (this SubscribeToStreamRequest) buildFilterOption(options *streams2.ReadReq_Options) {
	switch this.Filter.(type) {
	case SubscribeRequestFilter:
		options.FilterOption = this.buildFilter()

	case SubscribeRequestNoFilter:
		options.FilterOption = &streams2.ReadReq_Options_NoFilter{NoFilter: &shared.Empty{}}
	}
}

func (this SubscribeToStreamRequest) buildFilter() *streams2.ReadReq_Options_Filter {
	filter := this.Filter.(SubscribeRequestFilter)
	result := &streams2.ReadReq_Options_Filter{
		Filter: &streams2.ReadReq_Options_FilterOptions{
			CheckpointIntervalMultiplier: filter.CheckpointIntervalMultiplier,
		},
	}

	switch filter.FilterBy.(type) {
	case SubscribeRequestFilterByEventType:
		eventTypeFilter := filter.FilterBy.(SubscribeRequestFilterByEventType)

		result.Filter.Filter = &streams2.ReadReq_Options_FilterOptions_EventType{
			EventType: &streams2.ReadReq_Options_FilterOptions_Expression{
				Regex:  eventTypeFilter.Regex,
				Prefix: eventTypeFilter.Prefix,
			},
		}

	case SubscribeRequestFilterByStreamIdentifier:
		streamIdentifierFilter := filter.FilterBy.(SubscribeRequestFilterByStreamIdentifier)

		result.Filter.Filter = &streams2.ReadReq_Options_FilterOptions_StreamIdentifier{
			StreamIdentifier: &streams2.ReadReq_Options_FilterOptions_Expression{
				Regex:  streamIdentifierFilter.Regex,
				Prefix: streamIdentifierFilter.Prefix,
			},
		}
	}

	switch filter.Window.(type) {
	case SubscribeRequestFilterWindowMax:
		maxWindow := filter.Window.(SubscribeRequestFilterWindowMax)

		result.Filter.Window = &streams2.ReadReq_Options_FilterOptions_Max{
			Max: maxWindow.Max,
		}

	case SubscribeRequestFilterWindowCount:
		result.Filter.Window = &streams2.ReadReq_Options_FilterOptions_Count{
			Count: &shared.Empty{},
		}
	}

	return result
}

type isSubscribeRequestFilterOption interface {
	isSubscribeRequestFilterOption()
}

type SubscribeRequestFilter struct {
	// SubscribeRequestFilterByEventType
	// SubscribeRequestFilterByStreamIdentifier
	FilterBy isSubscribeRequestFilterBy
	// SubscribeRequestFilterWindowMax
	// SubscribeRequestFilterWindowCount
	Window                       isSubscribeRequestFilterWindow
	CheckpointIntervalMultiplier uint32
}

func (this SubscribeRequestFilter) isSubscribeRequestFilterOption() {}

type isSubscribeRequestFilterBy interface {
	isSubscribeRequestFilterBy()
}

type SubscribeRequestFilterByEventType struct {
	Regex  string
	Prefix []string
}

func (this SubscribeRequestFilterByEventType) isSubscribeRequestFilterBy() {}

type SubscribeRequestFilterByStreamIdentifier struct {
	Regex  string
	Prefix []string
}

func (this SubscribeRequestFilterByStreamIdentifier) isSubscribeRequestFilterBy() {}

type isSubscribeRequestFilterWindow interface {
	isSubscribeRequestFilterWindow()
}

type SubscribeRequestFilterWindowMax struct {
	Max uint32
}

func (this SubscribeRequestFilterWindowMax) isSubscribeRequestFilterWindow() {}

type SubscribeRequestFilterWindowCount struct{}

func (this SubscribeRequestFilterWindowCount) isSubscribeRequestFilterWindow() {}

type SubscribeRequestNoFilter struct{}

func (this SubscribeRequestNoFilter) isSubscribeRequestFilterOption() {}

type isSubscribeRequestStreamOptions interface {
	isSubscribeRequestStreamOptions()
}

type SubscribeRequestStreamOptions struct {
	StreamIdentifier string
	// SubscribeRequestOptionsStreamRevision
	// SubscribeRequestOptionsStreamRevisionStart
	// SubscribeRequestOptionsStreamRevisionEnd
	Revision IsSubscribeRequestStreamOptionsStreamRevision
}

func (this SubscribeRequestStreamOptions) isSubscribeRequestStreamOptions() {}

type IsSubscribeRequestStreamOptionsStreamRevision interface {
	isSubscribeRequestStreamOptionsStreamRevision()
}

type SubscribeRequestOptionsStreamRevision struct {
	Revision uint64
}

func (this SubscribeRequestOptionsStreamRevision) isSubscribeRequestStreamOptionsStreamRevision() {}

type SubscribeRequestOptionsStreamRevisionStart struct{}

func (this SubscribeRequestOptionsStreamRevisionStart) isSubscribeRequestStreamOptionsStreamRevision() {
}

type SubscribeRequestOptionsStreamRevisionEnd struct{}

func (this SubscribeRequestOptionsStreamRevisionEnd) isSubscribeRequestStreamOptionsStreamRevision() {
}

type SubscribeRequestStreamOptionsAll struct {
	// SubscribeRequestOptionsAllPosition
	// SubscribeRequestOptionsAllStartPosition
	// SubscribeRequestOptionsAllEndPosition
	Position IsSubscribeRequestOptionsAllPosition
}

func (this SubscribeRequestStreamOptionsAll) isSubscribeRequestStreamOptions() {}

type IsSubscribeRequestOptionsAllPosition interface {
	isSubscribeRequestOptionsAllPosition()
}

type SubscribeRequestOptionsAllPosition struct {
	CommitPosition  uint64
	PreparePosition uint64
}

func (this SubscribeRequestOptionsAllPosition) isSubscribeRequestOptionsAllPosition() {}

type SubscribeRequestOptionsAllStartPosition struct{}

func (this SubscribeRequestOptionsAllStartPosition) isSubscribeRequestOptionsAllPosition() {}

type SubscribeRequestOptionsAllEndPosition struct{}

func (this SubscribeRequestOptionsAllEndPosition) isSubscribeRequestOptionsAllPosition() {}

type SubscribeRequestDirection string

const (
	SubscribeRequestDirectionForward SubscribeRequestDirection = "SubscribeRequestDirectionForward"
	SubscribeRequestDirectionEnd     SubscribeRequestDirection = "SubscribeRequestDirectionEnd"
)
