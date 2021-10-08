package event_streams

import (
	"github.com/pivonroll/EventStore-Client-Go/protos/shared"
	"github.com/pivonroll/EventStore-Client-Go/protos/streams2"
)

type isFilter interface {
	isFilter()
}

// Filter is used to specify a filter when reading events from stream $all.
// Events can be filtered by event type or by a stream ID.
// Reading window can be specified to a maximum number of events to
// read by setting Window to FilterWindowMax.
// If you do not want to specify a maximum number of events to
// read set Window to FilterNoWindow.
// CheckpointIntervalMultiplier must be greater than zero.
// Default is 1 as specified in DefaultCheckpointIntervalMultiplier.
type Filter struct {
	// FilterByEventType
	// FilterByStreamId
	FilterBy isFilterBy
	// FilterWindowMax
	// FilterNoWindow
	Window                       isFilterWindow
	CheckpointIntervalMultiplier uint32
}

const (
	DefaultCheckpointIntervalMultiplier = uint32(1)
)

// DefaultFilterWindowMax returns a default value for a maximum number
// of events to read with filter from streams $all.
func DefaultFilterWindowMax() FilterWindowMax {
	return FilterWindowMax{
		Max: 32,
	}
}

func (this Filter) isFilter() {}

type isFilterBy interface {
	isFilterBy()
}

// FilterByEventType sets a filter matcher for stream $all to match by event type.
// Matching can be done by a set of prefixes or a regex.
type FilterByEventType struct {
	// PrefixFilterMatcher
	// RegexFilterMatcher
	Matcher filterMatcher
}

func (this FilterByEventType) isFilterBy() {}

// FilterByStreamId sets a filter matcher for stream $all to match by stream ID.
// Matching can be done by a set of prefixes or a regex.
type FilterByStreamId struct {
	// PrefixFilterMatcher
	// RegexFilterMatcher
	Matcher filterMatcher
}

func (this FilterByStreamId) isFilterBy() {}

type isFilterWindow interface {
	isFilterWindow()
}

// FilterWindowMax is a maximum number of events that we want to receive as a result.
type FilterWindowMax struct {
	Max uint32
}

func (this FilterWindowMax) isFilterWindow() {}

// FilterNoWindow is used when we do not want to set the maximum number of events to receive as a result.
type FilterNoWindow struct{}

func (this FilterNoWindow) isFilterWindow() {}

type noFilter struct{}

func (this noFilter) isFilter() {}

type filterMatcher interface {
	isFilterMatcher()
}

// RegexFilterMatcher represents a regex used to match event type or a stream identifier.
type RegexFilterMatcher struct {
	Regex string
}

func (regex RegexFilterMatcher) isFilterMatcher() {
}

// PrefixFilterMatcher represents a list of prefixes used to match against event type or a stream ID.
type PrefixFilterMatcher struct {
	PrefixList []string
}

func (prefix PrefixFilterMatcher) isFilterMatcher() {
}

func buildProtoFilter(filterInput isFilter) *streams2.ReadReq_Options_Filter {
	filter := filterInput.(Filter)
	result := &streams2.ReadReq_Options_Filter{
		Filter: &streams2.ReadReq_Options_FilterOptions{
			CheckpointIntervalMultiplier: filter.CheckpointIntervalMultiplier,
		},
	}

	switch filter.FilterBy.(type) {
	case FilterByEventType:
		eventTypeFilter := filter.FilterBy.(FilterByEventType)
		filterExpression := buildProtoFilterExpression(eventTypeFilter.Matcher)
		result.Filter.Filter = &streams2.ReadReq_Options_FilterOptions_EventType{
			EventType: filterExpression,
		}

	case FilterByStreamId:
		streamIdentifierFilter := filter.FilterBy.(FilterByStreamId)
		filterExpression := buildProtoFilterExpression(streamIdentifierFilter.Matcher)
		result.Filter.Filter = &streams2.ReadReq_Options_FilterOptions_StreamIdentifier{
			StreamIdentifier: filterExpression,
		}
	}

	switch filter.Window.(type) {
	case FilterWindowMax:
		maxWindow := filter.Window.(FilterWindowMax)

		result.Filter.Window = &streams2.ReadReq_Options_FilterOptions_Max{
			Max: maxWindow.Max,
		}

	case FilterNoWindow:
		result.Filter.Window = &streams2.ReadReq_Options_FilterOptions_Count{
			Count: &shared.Empty{},
		}
	}

	return result
}

func buildProtoFilterExpression(
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
