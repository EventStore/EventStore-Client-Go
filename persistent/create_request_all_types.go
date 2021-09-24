package persistent

import (
	"github.com/pivonroll/EventStore-Client-Go/protos/persistent"
	"github.com/pivonroll/EventStore-Client-Go/protos/shared"
)

type CreateAllRequest struct {
	GroupName string
	// AllPosition
	// AllPositionStart
	// AllPositionEnd
	Position isAllPosition
	// CreateRequestAllNoFilter
	// CreateRequestAllFilter
	Filter   IsCreateRequestAllFilter
	Settings CreateOrUpdateRequestSettings
}

func (request CreateAllRequest) Build() *persistent.CreateReq {
	streamOption := &persistent.CreateReq_Options_All{
		All: &persistent.CreateReq_AllOptions{},
	}

	request.Position.buildCreateRequestPosition(streamOption.All)
	request.Filter.build(streamOption.All)
	protoSettings := request.Settings.buildCreateRequestSettings()

	result := &persistent.CreateReq{
		Options: &persistent.CreateReq_Options{
			StreamOption: streamOption,
			GroupName:    request.GroupName,
			Settings:     protoSettings,
		},
	}

	return result
}

type IsCreateRequestAllFilter interface {
	isCreateRequestAllFilter()
	build(*persistent.CreateReq_AllOptions)
}

type CreateRequestAllNoFilter struct{}

func (c CreateRequestAllNoFilter) isCreateRequestAllFilter() {
}

func (c CreateRequestAllNoFilter) build(protoOptions *persistent.CreateReq_AllOptions) {
	protoOptions.FilterOption = &persistent.CreateReq_AllOptions_NoFilter{
		NoFilter: &shared.Empty{},
	}
}

type CreateRequestAllFilter struct {
	FilterBy CreateRequestAllFilterByType
	// CreateRequestAllFilterByRegex
	// CreateRequestAllFilterByPrefix
	Matcher isCreateRequestAllFilterMatcher
	// CreateRequestAllFilterWindowMax
	// CreateRequestAllFilterWindowCount
	Window                       isCreateRequestAllFilterWindow
	CheckpointIntervalMultiplier uint32
}

type CreateRequestAllFilterByType string

const (
	CreateRequestAllFilterByStreamIdentifier CreateRequestAllFilterByType = "CreateRequestAllFilterByStreamIdentifier"
	CreateRequestAllFilterByEventType        CreateRequestAllFilterByType = "CreateRequestAllFilterByEventType"
)

func (c CreateRequestAllFilter) isCreateRequestAllFilter() {
}

func (c CreateRequestAllFilter) build(protoOptions *persistent.CreateReq_AllOptions) {
	filter := &persistent.CreateReq_AllOptions_Filter{
		Filter: &persistent.CreateReq_AllOptions_FilterOptions{
			CheckpointIntervalMultiplier: c.CheckpointIntervalMultiplier,
		},
	}

	c.Window.build(filter.Filter)
	protoMatcher := c.Matcher.build()

	switch c.FilterBy {
	case CreateRequestAllFilterByEventType:
		filter.Filter.Filter = &persistent.CreateReq_AllOptions_FilterOptions_EventType{
			EventType: protoMatcher,
		}
	case CreateRequestAllFilterByStreamIdentifier:
		filter.Filter.Filter = &persistent.CreateReq_AllOptions_FilterOptions_StreamIdentifier{
			StreamIdentifier: protoMatcher,
		}
	}
	protoOptions.FilterOption = filter
}

type isCreateRequestAllFilterMatcher interface {
	isCreateRequestAllFilterByStreamIdentifierMatcher()
	build() *persistent.CreateReq_AllOptions_FilterOptions_Expression
}

type CreateRequestAllFilterByRegex struct {
	Regex string
}

func (c CreateRequestAllFilterByRegex) isCreateRequestAllFilterByStreamIdentifierMatcher() {
}

func (c CreateRequestAllFilterByRegex) build() *persistent.CreateReq_AllOptions_FilterOptions_Expression {
	return &persistent.CreateReq_AllOptions_FilterOptions_Expression{
		Regex: c.Regex,
	}
}

type CreateRequestAllFilterByPrefix struct {
	Prefix []string
}

func (c CreateRequestAllFilterByPrefix) isCreateRequestAllFilterByStreamIdentifierMatcher() {
}

func (c CreateRequestAllFilterByPrefix) build() *persistent.CreateReq_AllOptions_FilterOptions_Expression {
	return &persistent.CreateReq_AllOptions_FilterOptions_Expression{
		Prefix: c.Prefix,
	}
}

type isCreateRequestAllFilterWindow interface {
	isCreateRequestAllFilterWindow()
	build(*persistent.CreateReq_AllOptions_FilterOptions)
}

type CreateRequestAllFilterWindowMax struct {
	Max uint32
}

func (c CreateRequestAllFilterWindowMax) isCreateRequestAllFilterWindow() {
}

func (c CreateRequestAllFilterWindowMax) build(options *persistent.CreateReq_AllOptions_FilterOptions) {
	options.Window = &persistent.CreateReq_AllOptions_FilterOptions_Max{
		Max: c.Max,
	}
}

type CreateRequestAllFilterWindowCount struct{}

func (c CreateRequestAllFilterWindowCount) isCreateRequestAllFilterWindow() {
}

func (c CreateRequestAllFilterWindowCount) build(options *persistent.CreateReq_AllOptions_FilterOptions) {
	options.Window = &persistent.CreateReq_AllOptions_FilterOptions_Count{
		Count: &shared.Empty{},
	}
}
