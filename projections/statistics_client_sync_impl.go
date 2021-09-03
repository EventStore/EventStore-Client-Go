package projections

import "github.com/EventStore/EventStore-Client-Go/protos/projections"

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
