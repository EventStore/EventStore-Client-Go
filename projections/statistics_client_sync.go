package projections

import "github.com/EventStore/EventStore-Client-Go/protos/projections"

type StatisticsClientSync interface {
	Read() (StatisticsClientResponse, error)
}

const (
	StatisticsStatusAborted = "Stopped"
	StatisticsStatusStopped = "Aborted/Stopped"
	StatisticsStatusRunning = "Running"
)

const StatisticsModeOneTime = "OneTime"

type StatisticsClientResponse struct {
	CoreProcessingTime                 int64
	Version                            int64
	Epoch                              int64
	EffectiveName                      string
	WritesInProgress                   int32
	ReadsInProgress                    int32
	PartitionsCached                   int32
	Status                             string
	StateReason                        string
	Name                               string
	Mode                               string
	Position                           string
	Progress                           float32
	LastCheckpoint                     string
	EventsProcessedAfterRestart        int64
	CheckpointStatus                   string
	BufferedEvents                     int64
	WritePendingEventsBeforeCheckpoint int32
	WritePendingEventsAfterCheckpoint  int32
}

type StatisticsClientSyncImpl struct {
	client projections.Projections_StatisticsClient
}

func (statisticsSync *StatisticsClientSyncImpl) Read() (StatisticsClientResponse, error) {
	result, err := statisticsSync.client.Recv()
	if err != nil {
		return StatisticsClientResponse{}, err
	}

	return StatisticsClientResponse{
		CoreProcessingTime:                 result.Details.CoreProcessingTime,
		Version:                            result.Details.Version,
		Epoch:                              result.Details.Epoch,
		EffectiveName:                      result.Details.EffectiveName,
		WritesInProgress:                   result.Details.WritesInProgress,
		ReadsInProgress:                    result.Details.ReadsInProgress,
		PartitionsCached:                   result.Details.PartitionsCached,
		Status:                             result.Details.Status,
		StateReason:                        result.Details.StateReason,
		Name:                               result.Details.Name,
		Mode:                               result.Details.Mode,
		Position:                           result.Details.Position,
		Progress:                           result.Details.Progress,
		LastCheckpoint:                     result.Details.LastCheckpoint,
		EventsProcessedAfterRestart:        result.Details.EventsProcessedAfterRestart,
		CheckpointStatus:                   result.Details.CheckpointStatus,
		BufferedEvents:                     result.Details.BufferedEvents,
		WritePendingEventsBeforeCheckpoint: result.Details.WritePendingEventsBeforeCheckpoint,
		WritePendingEventsAfterCheckpoint:  result.Details.WritePendingEventsAfterCheckpoint,
	}, nil
}

func newStatisticsClientSyncImpl(client projections.Projections_StatisticsClient) *StatisticsClientSyncImpl {
	return &StatisticsClientSyncImpl{
		client: client,
	}
}
