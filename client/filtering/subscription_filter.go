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
