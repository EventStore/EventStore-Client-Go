package projections

import (
	"github.com/pivonroll/EventStore-Client-Go/errors"
	"github.com/pivonroll/EventStore-Client-Go/protos/projections"
)

//go:generate mockgen -source=statistics_client_sync.go -destination=statistics_client_sync_mock.go -package=projections

type StatisticsClientSync interface {
	Read() (StatisticsClientResponse, errors.Error)
}

type statisticsClientSyncFactory interface {
	Create(client projections.Projections_StatisticsClient) StatisticsClientSync
}

type statisticsClientSyncFactoryImpl struct{}

func (factory statisticsClientSyncFactoryImpl) Create(
	statisticsClient projections.Projections_StatisticsClient) StatisticsClientSync {
	return newStatisticsClientSyncImpl(statisticsClient)
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
