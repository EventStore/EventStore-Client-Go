package systemmetadata

type SystemStream string

const (
	AllStream         SystemStream = "$all"
	StreamsStream     SystemStream = "$streams"
	SettingsStream    SystemStream = "$settings"
	StatsStreamPrefix SystemStream = "$stats"
)

func IsSystemStream(streamId string) bool {
	return len(streamId) > 0 && streamId[0] == '$'
}
