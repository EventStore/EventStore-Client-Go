package systemmetadata

type SystemStream string

const (
	AllStream         SystemStream = "$all"
	StreamsStream     SystemStream = "$streams"
	SettingsStream    SystemStream = "$settings"
	StatsStreamPrefix SystemStream = "$stats"
)
