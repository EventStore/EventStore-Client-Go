package projections

import (
	"io"

	"github.com/pivonroll/EventStore-Client-Go/connection"
	"github.com/pivonroll/EventStore-Client-Go/errors"
	"github.com/pivonroll/EventStore-Client-Go/protos/projections"
)

type StatisticsClientSyncImpl struct {
	client             projections.Projections_StatisticsClient
	readRequestChannel chan chan statisticsReadResult
}

type statisticsReadResult struct {
	statisticsClientResponse StatisticsClientResponse
	err                      errors.Error
}

func (this *StatisticsClientSyncImpl) Read() (StatisticsClientResponse, errors.Error) {
	channel := make(chan statisticsReadResult)

	this.readRequestChannel <- channel
	resp := <-channel

	return resp.statisticsClientResponse, resp.err
}

func (statisticsSync *StatisticsClientSyncImpl) readOne() (StatisticsClientResponse, errors.Error) {
	result, protoErr := statisticsSync.client.Recv()
	if protoErr != nil {
		if protoErr == io.EOF {
			return StatisticsClientResponse{}, errors.NewError(errors.EndOfStream, protoErr)
		}
		trailer := statisticsSync.client.Trailer()
		err := connection.GetErrorFromProtoException(trailer, protoErr)
		if err != nil {
			return StatisticsClientResponse{}, err
		}
		return StatisticsClientResponse{}, errors.NewError(errors.FatalError, protoErr)
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

func (statisticsSync *StatisticsClientSyncImpl) readLoop() {
	for {
		responseChannel := <-statisticsSync.readRequestChannel
		result, err := statisticsSync.readOne()

		response := statisticsReadResult{
			statisticsClientResponse: result,
			err:                      err,
		}

		responseChannel <- response
	}
}

func newStatisticsClientSyncImpl(client projections.Projections_StatisticsClient) *StatisticsClientSyncImpl {
	statisticsReadClient := &StatisticsClientSyncImpl{
		client:             client,
		readRequestChannel: make(chan chan statisticsReadResult),
	}

	go statisticsReadClient.readLoop()

	return statisticsReadClient
}
