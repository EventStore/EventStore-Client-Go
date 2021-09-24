package projections

import (
	"strings"

	"github.com/pivonroll/EventStore-Client-Go/protos/projections"
	"github.com/pivonroll/EventStore-Client-Go/protos/shared"
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
