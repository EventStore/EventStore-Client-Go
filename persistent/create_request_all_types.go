package persistent

import (
	"github.com/EventStore/EventStore-Client-Go/protos/persistent"
	"github.com/EventStore/EventStore-Client-Go/protos/shared"
)

type CreateRequestAll struct {
	GroupName string
	// CreateRequestAllPosition
	// CreateRequestAllPositionStart
	// CreateRequestAllPositionEnd
	Position IsCreateRequestAllPosition
	// CreateRequestAllNoFilter
	// CreateRequestAllFilter
	Filter   IsCreateRequestAllFilter
	Settings CreateRequestSettings
}

func (request CreateRequestAll) Build() *persistent.CreateReq {
	streamOption := &persistent.CreateReq_Options_All{
		All: &persistent.CreateReq_AllOptions{},
	}

	request.Position.build(streamOption.All)
	request.Filter.build(streamOption.All)
	protoSettings := request.Settings.build()

	result := &persistent.CreateReq{
		Options: &persistent.CreateReq_Options{
			StreamOption: streamOption,
			GroupName:    request.GroupName,
			Settings:     protoSettings,
		},
	}

	return result
}

type IsCreateRequestAllPosition interface {
	isCreateRequestAllPosition()
	build(*persistent.CreateReq_AllOptions)
}

type CreateRequestAllPosition struct {
	Commit  uint64
	Prepare uint64
}

func (c CreateRequestAllPosition) isCreateRequestAllPosition() {}

func (c CreateRequestAllPosition) build(protoOptions *persistent.CreateReq_AllOptions) {
	protoOptions.AllOption = &persistent.CreateReq_AllOptions_Position{
		Position: &persistent.CreateReq_Position{
			CommitPosition:  c.Commit,
			PreparePosition: c.Prepare,
		},
	}
}

type CreateRequestAllPositionStart struct{}

func (c CreateRequestAllPositionStart) isCreateRequestAllPosition() {
}

func (c CreateRequestAllPositionStart) build(protoOptions *persistent.CreateReq_AllOptions) {
	protoOptions.AllOption = &persistent.CreateReq_AllOptions_Start{
		Start: &shared.Empty{},
	}
}

type CreateRequestAllPositionEnd struct{}

func (c CreateRequestAllPositionEnd) isCreateRequestAllPosition() {
}

func (c CreateRequestAllPositionEnd) build(protoOptions *persistent.CreateReq_AllOptions) {
	protoOptions.AllOption = &persistent.CreateReq_AllOptions_End{
		End: &shared.Empty{},
	}
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
