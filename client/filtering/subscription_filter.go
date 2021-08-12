package filtering

type FilterType int

const (
	EventFilter       FilterType = 0
	StreamFilter      FilterType = 1
	NoMaxSearchWindow int        = -1
)

type SubscriptionFilterOptions struct {
	MaxSearchWindow    int
	CheckpointInterval int
	SubscriptionFilter SubscriptionFilter
}
type SubscriptionFilter struct {
	FilterType FilterType
	Prefixes   []string
	Regex      string
}

func NewDefaultSubscriptionFilterOptions(filter SubscriptionFilter) SubscriptionFilterOptions {
	return SubscriptionFilterOptions{
		MaxSearchWindow:    32,
		CheckpointInterval: 1,
		SubscriptionFilter: filter,
	}
}

func NewSubscriptionFilterOptions(maxSearchWindow int, checkpointInterval int, filter SubscriptionFilter) SubscriptionFilterOptions {
	return SubscriptionFilterOptions{
		MaxSearchWindow:    maxSearchWindow,
		CheckpointInterval: checkpointInterval,
		SubscriptionFilter: filter,
	}
}

func NewEventPrefixFilter(prefixes []string) SubscriptionFilter {
	return SubscriptionFilter{
		FilterType: EventFilter,
		Prefixes:   prefixes,
	}
}

func NewEventRegexFilter(regex string) SubscriptionFilter {
	return SubscriptionFilter{
		FilterType: EventFilter,
		Regex:      regex,
	}
}

func NewStreamPrefixFilter(prefixes []string) SubscriptionFilter {
	return SubscriptionFilter{
		FilterType: StreamFilter,
		Prefixes:   prefixes,
	}
}

func NewStreamRegexFilter(regex string) SubscriptionFilter {
	return SubscriptionFilter{
		FilterType: StreamFilter,
		Regex:      regex,
	}
}
