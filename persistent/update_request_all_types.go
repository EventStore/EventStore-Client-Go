package persistent

import "github.com/pivonroll/EventStore-Client-Go/protos/persistent"

type UpdateAllRequest struct {
	GroupName string
	// AllPosition
	// AllPositionStart
	// AllPositionEnd
	Position isAllPosition
	Settings CreateOrUpdateRequestSettings
}

func (request UpdateAllRequest) Build() *persistent.UpdateReq {
	streamOption := &persistent.UpdateReq_Options_All{
		All: &persistent.UpdateReq_AllOptions{},
	}

	request.Position.buildUpdateRequestPosition(streamOption.All)
	protoSettings := request.Settings.buildUpdateRequestSettings()

	result := &persistent.UpdateReq{
		Options: &persistent.UpdateReq_Options{
			StreamOption: streamOption,
			GroupName:    request.GroupName,
			Settings:     protoSettings,
		},
	}

	return result
}
