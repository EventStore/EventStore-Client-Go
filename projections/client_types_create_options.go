package projections

import (
	"strings"

	"github.com/EventStore/EventStore-Client-Go/protos/projections"
	"github.com/EventStore/EventStore-Client-Go/protos/shared"
)

type CreateConfigModeType string

const (
	CreateConfigModeOneTimeOptionType    CreateConfigModeType = "CreateConfigModeOneTimeOptionType"
	CreateConfigModeTransientOptionType  CreateConfigModeType = "CreateConfigModeTransientOptionType"
	CreateConfigModeContinuousOptionType CreateConfigModeType = "CreateConfigModeContinuousOptionType"
)

type CreateConfigMode interface {
	GetType() CreateConfigModeType
}

type CreateConfigModeOneTimeOption struct{}

func (mode CreateConfigModeOneTimeOption) GetType() CreateConfigModeType {
	return CreateConfigModeOneTimeOptionType
}

type CreateConfigModeTransientOption struct {
	Name string
}

func (mode CreateConfigModeTransientOption) GetType() CreateConfigModeType {
	return CreateConfigModeTransientOptionType
}

type CreateConfigModeContinuousOption struct {
	Name                string
	TrackEmittedStreams bool
}

func (mode CreateConfigModeContinuousOption) GetType() CreateConfigModeType {
	return CreateConfigModeContinuousOptionType
}

type CreateOptionsRequest struct {
	mode  CreateConfigMode
	query string
}

func (createConfig *CreateOptionsRequest) SetQuery(query string) *CreateOptionsRequest {
	createConfig.query = query
	return createConfig
}

func (createConfig *CreateOptionsRequest) SetMode(mode CreateConfigMode) *CreateOptionsRequest {
	createConfig.mode = mode
	return createConfig
}

func (createConfig *CreateOptionsRequest) Build() *projections.CreateReq {
	if strings.TrimSpace(createConfig.query) == "" {
		panic("Failed to build CreateOptionsRequest. Trimmed query is an empty string")
	}

	result := &projections.CreateReq{
		Options: &projections.CreateReq_Options{
			Mode:  nil,
			Query: createConfig.query,
		},
	}

	if createConfig.mode.GetType() == CreateConfigModeOneTimeOptionType {
		result.Options.Mode = &projections.CreateReq_Options_OneTime{
			OneTime: &shared.Empty{},
		}
	} else if createConfig.mode.GetType() == CreateConfigModeTransientOptionType {
		transientOption := createConfig.mode.(CreateConfigModeTransientOption)
		result.Options.Mode = &projections.CreateReq_Options_Transient_{
			Transient: &projections.CreateReq_Options_Transient{
				Name: transientOption.Name,
			},
		}
	} else if createConfig.mode.GetType() == CreateConfigModeContinuousOptionType {
		continuousOption := createConfig.mode.(CreateConfigModeContinuousOption)
		result.Options.Mode = &projections.CreateReq_Options_Continuous_{
			Continuous: &projections.CreateReq_Options_Continuous{
				Name:                continuousOption.Name,
				TrackEmittedStreams: continuousOption.TrackEmittedStreams,
			},
		}
	}

	return result
}

type UpdateOptionsEmitOptionType string

const (
	UpdateOptionsEmitOptionEnabledType UpdateOptionsEmitOptionType = "UpdateOptionsEmitOptionEnabledType"
	UpdateOptionsEmitOptionNoEmitType  UpdateOptionsEmitOptionType = "UpdateOptionsEmitOptionNoEmitType"
)

type UpdateOptionsEmitOption interface {
	GetType() UpdateOptionsEmitOptionType
}

type UpdateOptionsEmitOptionEnabled struct {
	EmitEnabled bool
}

func (u UpdateOptionsEmitOptionEnabled) GetType() UpdateOptionsEmitOptionType {
	return UpdateOptionsEmitOptionEnabledType
}

type UpdateOptionsEmitOptionNoEmit struct{}

func (u UpdateOptionsEmitOptionNoEmit) GetType() UpdateOptionsEmitOptionType {
	return UpdateOptionsEmitOptionNoEmitType
}

type UpdateOptionsRequest struct {
	emitOption UpdateOptionsEmitOption
	query      string
	name       string
}

func (updateConfig *UpdateOptionsRequest) SetQuery(query string) *UpdateOptionsRequest {
	updateConfig.query = query
	return updateConfig
}

func (updateConfig *UpdateOptionsRequest) SetName(name string) *UpdateOptionsRequest {
	updateConfig.name = name
	return updateConfig
}

func (updateConfig *UpdateOptionsRequest) SetEmitOption(option UpdateOptionsEmitOption) *UpdateOptionsRequest {
	updateConfig.emitOption = option
	return updateConfig
}

func (updateConfig *UpdateOptionsRequest) Build() *projections.UpdateReq {
	if strings.TrimSpace(updateConfig.name) == "" {
		panic("Failed to build UpdateOptionsRequest. Trimmed name is an empty string")
	}

	if strings.TrimSpace(updateConfig.query) == "" {
		panic("Failed to build UpdateOptionsRequest. Trimmed query is an empty string")
	}

	result := &projections.UpdateReq{
		Options: &projections.UpdateReq_Options{
			Name:  updateConfig.name,
			Query: updateConfig.query,
		},
	}

	if updateConfig.emitOption.GetType() == UpdateOptionsEmitOptionNoEmitType {
		result.Options.EmitOption = &projections.UpdateReq_Options_NoEmitOptions{
			NoEmitOptions: &shared.Empty{},
		}
	} else if updateConfig.emitOption.GetType() == UpdateOptionsEmitOptionEnabledType {
		emitOption := updateConfig.emitOption.(UpdateOptionsEmitOptionEnabled)
		result.Options.EmitOption = &projections.UpdateReq_Options_EmitEnabled{
			EmitEnabled: emitOption.EmitEnabled,
		}
	}

	return result
}

type DeleteOptionsRequest struct {
	name                   string
	deleteEmittedStreams   bool
	deleteStateStream      bool
	deleteCheckpointStream bool
}

func (deleteOptions *DeleteOptionsRequest) SetName(name string) *DeleteOptionsRequest {
	deleteOptions.name = name
	return deleteOptions
}

func (deleteOptions *DeleteOptionsRequest) SetDeleteEmittedStreams(delete bool) *DeleteOptionsRequest {
	deleteOptions.deleteEmittedStreams = delete
	return deleteOptions
}

func (deleteOptions *DeleteOptionsRequest) SetDeleteStateStream(delete bool) *DeleteOptionsRequest {
	deleteOptions.deleteStateStream = delete
	return deleteOptions
}

func (deleteOptions *DeleteOptionsRequest) SetDeleteCheckpointStream(delete bool) *DeleteOptionsRequest {
	deleteOptions.deleteCheckpointStream = delete
	return deleteOptions
}

func (deleteOptions *DeleteOptionsRequest) Build() *projections.DeleteReq {
	result := &projections.DeleteReq{
		Options: &projections.DeleteReq_Options{
			Name:                   deleteOptions.name,
			DeleteEmittedStreams:   deleteOptions.deleteEmittedStreams,
			DeleteStateStream:      deleteOptions.deleteStateStream,
			DeleteCheckpointStream: deleteOptions.deleteCheckpointStream,
		},
	}

	return result
}

type StatisticsOptionsRequestModeType string

const (
	StatisticsOptionsRequestModeAllType        StatisticsOptionsRequestModeType = "StatisticsOptionsRequestModeAllType"
	StatisticsOptionsRequestModeNameType       StatisticsOptionsRequestModeType = "StatisticsOptionsRequestModeNameType"
	StatisticsOptionsRequestModeTransientType  StatisticsOptionsRequestModeType = "StatisticsOptionsRequestModeTransientType"
	StatisticsOptionsRequestModeContinuousType StatisticsOptionsRequestModeType = "StatisticsOptionsRequestModeContinuousType"
	StatisticsOptionsRequestModeOneTimeType    StatisticsOptionsRequestModeType = "StatisticsOptionsRequestModeOneTimeType"
)

type StatisticsOptionsRequestMode interface {
	GetType() StatisticsOptionsRequestModeType
}

type StatisticsOptionsRequestModeAll struct{}

func (s StatisticsOptionsRequestModeAll) GetType() StatisticsOptionsRequestModeType {
	return StatisticsOptionsRequestModeAllType
}

type StatisticsOptionsRequestModeName struct {
	Name string
}

func (s StatisticsOptionsRequestModeName) GetType() StatisticsOptionsRequestModeType {
	return StatisticsOptionsRequestModeNameType
}

type StatisticsOptionsRequestModeTransient struct{}

func (s StatisticsOptionsRequestModeTransient) GetType() StatisticsOptionsRequestModeType {
	return StatisticsOptionsRequestModeTransientType
}

type StatisticsOptionsRequestModeContinuous struct{}

func (s StatisticsOptionsRequestModeContinuous) GetType() StatisticsOptionsRequestModeType {
	return StatisticsOptionsRequestModeContinuousType
}

type StatisticsOptionsRequestModeOneTime struct{}

func (s StatisticsOptionsRequestModeOneTime) GetType() StatisticsOptionsRequestModeType {
	return StatisticsOptionsRequestModeOneTimeType
}

type StatisticsOptionsRequest struct {
	mode StatisticsOptionsRequestMode
}

func (statisticsOptions *StatisticsOptionsRequest) SetMode(mode StatisticsOptionsRequestMode) *StatisticsOptionsRequest {
	statisticsOptions.mode = mode
	return statisticsOptions
}

func (statisticsOptions *StatisticsOptionsRequest) Build() *projections.StatisticsReq {
	result := &projections.StatisticsReq{
		Options: &projections.StatisticsReq_Options{
			Mode: nil,
		},
	}

	if statisticsOptions.mode.GetType() == StatisticsOptionsRequestModeAllType {
		result.Options.Mode = &projections.StatisticsReq_Options_All{
			All: &shared.Empty{},
		}
	} else if statisticsOptions.mode.GetType() == StatisticsOptionsRequestModeTransientType {
		result.Options.Mode = &projections.StatisticsReq_Options_Transient{
			Transient: &shared.Empty{},
		}
	} else if statisticsOptions.mode.GetType() == StatisticsOptionsRequestModeContinuousType {
		result.Options.Mode = &projections.StatisticsReq_Options_Continuous{
			Continuous: &shared.Empty{},
		}
	} else if statisticsOptions.mode.GetType() == StatisticsOptionsRequestModeOneTimeType {
		result.Options.Mode = &projections.StatisticsReq_Options_OneTime{
			OneTime: &shared.Empty{},
		}
	} else if statisticsOptions.mode.GetType() == StatisticsOptionsRequestModeNameType {
		mode := statisticsOptions.mode.(StatisticsOptionsRequestModeName)
		result.Options.Mode = &projections.StatisticsReq_Options_Name{
			Name: mode.Name,
		}
	}

	return result
}

type DisableOptionsRequest struct {
	name string
}

func (disableOptionsRequest *DisableOptionsRequest) SetName(name string) *DisableOptionsRequest {
	disableOptionsRequest.name = name
	return disableOptionsRequest
}

func (disableOptionsRequest *DisableOptionsRequest) Build() *projections.DisableReq {
	result := &projections.DisableReq{
		Options: &projections.DisableReq_Options{
			Name:            disableOptionsRequest.name,
			WriteCheckpoint: false,
		},
	}

	return result
}

type AbortOptionsRequest struct {
	name string
}

func (abortOptionsRequest *AbortOptionsRequest) SetName(name string) *AbortOptionsRequest {
	abortOptionsRequest.name = name
	return abortOptionsRequest
}

func (abortOptionsRequest *AbortOptionsRequest) Build() *projections.DisableReq {
	result := &projections.DisableReq{
		Options: &projections.DisableReq_Options{
			Name:            abortOptionsRequest.name,
			WriteCheckpoint: true,
		},
	}

	return result
}

type EnableOptionsRequest struct {
	name            string
	writeCheckpoint bool
}

func (enableOptionsRequest *EnableOptionsRequest) SetName(name string) *EnableOptionsRequest {
	enableOptionsRequest.name = name
	return enableOptionsRequest
}

func (enableOptionsRequest *EnableOptionsRequest) Build() *projections.EnableReq {
	result := &projections.EnableReq{
		Options: &projections.EnableReq_Options{
			Name: enableOptionsRequest.name,
		},
	}

	return result
}

type ResetOptionsRequest struct {
	name            string
	writeCheckpoint bool
}

func (resetOptionsRequest *ResetOptionsRequest) SetName(name string) *ResetOptionsRequest {
	resetOptionsRequest.name = name
	return resetOptionsRequest
}

func (resetOptionsRequest *ResetOptionsRequest) SetWriteCheckpoint(writeCheckpoint bool) *ResetOptionsRequest {
	resetOptionsRequest.writeCheckpoint = writeCheckpoint
	return resetOptionsRequest
}

func (resetOptionsRequest *ResetOptionsRequest) Build() *projections.ResetReq {
	result := &projections.ResetReq{
		Options: &projections.ResetReq_Options{
			Name:            resetOptionsRequest.name,
			WriteCheckpoint: resetOptionsRequest.writeCheckpoint,
		},
	}

	return result
}

type StateOptionsRequest struct {
	name      string
	partition string
}

func (stateOptionsRequest *StateOptionsRequest) SetName(name string) *StateOptionsRequest {
	stateOptionsRequest.name = name
	return stateOptionsRequest
}

func (stateOptionsRequest *StateOptionsRequest) SetPartition(partition string) *StateOptionsRequest {
	stateOptionsRequest.partition = partition
	return stateOptionsRequest
}

func (stateOptionsRequest *StateOptionsRequest) Build() *projections.StateReq {
	result := &projections.StateReq{
		Options: &projections.StateReq_Options{
			Name:      stateOptionsRequest.name,
			Partition: stateOptionsRequest.partition,
		},
	}

	return result
}

type ResultOptionsRequest struct {
	name      string
	partition string
}

func (resultOptionsRequest *ResultOptionsRequest) SetName(name string) *ResultOptionsRequest {
	resultOptionsRequest.name = name
	return resultOptionsRequest
}

func (resultOptionsRequest *ResultOptionsRequest) SetPartition(partition string) *ResultOptionsRequest {
	resultOptionsRequest.partition = partition
	return resultOptionsRequest
}

func (resultOptionsRequest *ResultOptionsRequest) Build() *projections.ResultReq {
	result := &projections.ResultReq{
		Options: &projections.ResultReq_Options{
			Name:      resultOptionsRequest.name,
			Partition: resultOptionsRequest.partition,
		},
	}

	return result
}
