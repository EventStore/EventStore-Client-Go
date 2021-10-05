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
	Filter IsSubscribeRequestFilterOption
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
	case ReadPositionAll:
		allPosition := all.Position.(ReadPositionAll)
		result.All.AllOption = &streams2.ReadReq_Options_AllOptions_Position{
			Position: &streams2.ReadReq_Options_Position{
				CommitPosition:  allPosition.CommitPosition,
				PreparePosition: allPosition.PreparePosition,
			},
		}
	case ReadPositionAllStart:
		result.All.AllOption = &streams2.ReadReq_Options_AllOptions_Start{
			Start: &shared.Empty{},
		}
	case ReadPositionAllEnd:
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
	case ReadStreamRevision:
		streamRevision := streamOptions.Revision.(ReadStreamRevision)
		result.Stream.RevisionOption = &streams2.ReadReq_Options_StreamOptions_Revision{
			Revision: streamRevision.Revision,
		}
	case ReadStreamRevisionStart:
		result.Stream.RevisionOption = &streams2.ReadReq_Options_StreamOptions_Start{
			Start: &shared.Empty{},
		}
	case ReadStreamRevisionEnd:
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
		filterExpression := this.buildFilterExpression(eventTypeFilter.Matcher)
		result.Filter.Filter = &streams2.ReadReq_Options_FilterOptions_EventType{
			EventType: filterExpression,
		}

	case SubscribeRequestFilterByStreamIdentifier:
		streamIdentifierFilter := filter.FilterBy.(SubscribeRequestFilterByStreamIdentifier)
		filterExpression := this.buildFilterExpression(streamIdentifierFilter.Matcher)
		result.Filter.Filter = &streams2.ReadReq_Options_FilterOptions_StreamIdentifier{
			StreamIdentifier: filterExpression,
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

func (this SubscribeToStreamRequest) buildFilterExpression(
	matcher filterMatcher) *streams2.ReadReq_Options_FilterOptions_Expression {
	result := &streams2.ReadReq_Options_FilterOptions_Expression{}
	if regexMatcher, ok := matcher.(RegexFilterMatcher); ok {
		result.Regex = regexMatcher.Regex
	} else if prefixMatcher, ok := matcher.(PrefixFilterMatcher); ok {
		result.Prefix = prefixMatcher.PrefixList
	} else {
		panic("Invalid type received")
	}
	return result
}

type IsSubscribeRequestFilterOption interface {
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
	Matcher filterMatcher
}

func (this SubscribeRequestFilterByEventType) isSubscribeRequestFilterBy() {}

type SubscribeRequestFilterByStreamIdentifier struct {
	Matcher filterMatcher
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
	// ReadStreamRevision
	// ReadStreamRevisionStart
	// ReadStreamRevisionEnd
	Revision IsReadStreamRevision
}

func (this SubscribeRequestStreamOptions) isSubscribeRequestStreamOptions() {}

type SubscribeRequestStreamOptionsAll struct {
	// ReadPositionAll
	// ReadPositionAllStart
	// ReadPositionAllEnd
	Position IsReadPositionAll
}

func (this SubscribeRequestStreamOptionsAll) isSubscribeRequestStreamOptions() {}

type SubscribeRequestDirection string

const (
	SubscribeRequestDirectionForward SubscribeRequestDirection = "SubscribeRequestDirectionForward"
	SubscribeRequestDirectionEnd     SubscribeRequestDirection = "SubscribeRequestDirectionEnd"
)

type filterMatcher interface {
	isFilterMatcher()
}

type RegexFilterMatcher struct {
	Regex string
}

func (regex RegexFilterMatcher) isFilterMatcher() {
}

type PrefixFilterMatcher struct {
	PrefixList []string
}

func (prefix PrefixFilterMatcher) isFilterMatcher() {
}
