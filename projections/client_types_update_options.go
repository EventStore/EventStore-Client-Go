package projections

import (
	"strings"

	"github.com/EventStore/EventStore-Client-Go/protos/projections"
	"github.com/EventStore/EventStore-Client-Go/protos/shared"
)

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
