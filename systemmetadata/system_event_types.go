package systemmetadata

type SystemEventType string

const (
	SystemEventStreamDeleted   SystemEventType = "$streamDeleted"
	SystemEventStatsCollection SystemEventType = "$statsCollected"
	SystemEventLinkTo          SystemEventType = "$>"
	SystemEventStreamMetadata  SystemEventType = "$metadata"
	SystemEventSettings        SystemEventType = "$settings"
)
