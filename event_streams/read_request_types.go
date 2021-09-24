package event_streams

import (
	"github.com/pivonroll/EventStore-Client-Go/protos/shared"
	"github.com/pivonroll/EventStore-Client-Go/protos/streams2"
)

const ReadCountMax = ^uint64(0)

type ReadRequest struct {
	// ReadRequestStreamOptions
	// ReadRequestStreamOptionsAll
	StreamOption isReadRequestStreamOptions
	Direction    ReadRequestDirection
	ResolveLinks bool
	Count        uint64
	// ReadRequestFilter
	// ReadRequestNoFilter
	Filter isReadRequestFilterOption
}

func (this ReadRequest) Build() *streams2.ReadReq {
	result := &streams2.ReadReq{
		Options: &streams2.ReadReq_Options{
			ResolveLinks: this.ResolveLinks,
			FilterOption: nil,
			UuidOption: &streams2.ReadReq_Options_UUIDOption{
				Content: &streams2.ReadReq_Options_UUIDOption_String_{
					String_: &shared.Empty{},
				},
			},
			CountOption: &streams2.ReadReq_Options_Count{
				Count: this.Count,
			},
		},
	}

	if this.Direction == ReadRequestDirectionForward {
		result.Options.ReadDirection = streams2.ReadReq_Options_Forwards
	} else {
		result.Options.ReadDirection = streams2.ReadReq_Options_Backwards
	}

	this.buildStreamOption(result.Options)
	this.buildFilterOption(result.Options)

	return result
}

func (this ReadRequest) buildStreamOption(options *streams2.ReadReq_Options) {
	switch this.StreamOption.(type) {
	case ReadRequestStreamOptions:
		options.StreamOption = this.buildStreamOptions(
			this.StreamOption.(ReadRequestStreamOptions))
	case ReadRequestStreamOptionsAll:
		options.StreamOption = thisBuildStreamOptionAll(
			this.StreamOption.(ReadRequestStreamOptionsAll))
	}
}

func thisBuildStreamOptionAll(all ReadRequestStreamOptionsAll) *streams2.ReadReq_Options_All {
	result := &streams2.ReadReq_Options_All{
		All: &streams2.ReadReq_Options_AllOptions{},
	}

	switch all.Position.(type) {
	case ReadRequestOptionsAllPosition:
		allPosition := all.Position.(ReadRequestOptionsAllPosition)
		result.All.AllOption = &streams2.ReadReq_Options_AllOptions_Position{
			Position: &streams2.ReadReq_Options_Position{
				CommitPosition:  allPosition.CommitPosition,
				PreparePosition: allPosition.PreparePosition,
			},
		}
	case ReadRequestOptionsAllStartPosition:
		result.All.AllOption = &streams2.ReadReq_Options_AllOptions_Start{
			Start: &shared.Empty{},
		}
	case ReadRequestOptionsAllEndPosition:
		result.All.AllOption = &streams2.ReadReq_Options_AllOptions_End{
			End: &shared.Empty{},
		}
	}

	return result
}

func (this ReadRequest) buildStreamOptions(
	streamOptions ReadRequestStreamOptions) *streams2.ReadReq_Options_Stream {
	result := &streams2.ReadReq_Options_Stream{
		Stream: &streams2.ReadReq_Options_StreamOptions{
			StreamIdentifier: &shared.StreamIdentifier{
				StreamName: []byte(streamOptions.StreamIdentifier),
			},
			RevisionOption: nil,
		},
	}

	switch streamOptions.Revision.(type) {
	case ReadRequestOptionsStreamRevision:
		streamRevision := streamOptions.Revision.(ReadRequestOptionsStreamRevision)
		result.Stream.RevisionOption = &streams2.ReadReq_Options_StreamOptions_Revision{
			Revision: streamRevision.Revision,
		}
	case ReadRequestOptionsStreamRevisionStart:
		result.Stream.RevisionOption = &streams2.ReadReq_Options_StreamOptions_Start{
			Start: &shared.Empty{},
		}
	case ReadRequestOptionsStreamRevisionEnd:
		result.Stream.RevisionOption = &streams2.ReadReq_Options_StreamOptions_End{
			End: &shared.Empty{},
		}
	}

	return result
}

func (this ReadRequest) buildFilterOption(options *streams2.ReadReq_Options) {
	switch this.Filter.(type) {
	case ReadRequestFilter:
		options.FilterOption = this.buildFilter()

	case ReadRequestNoFilter:
		options.FilterOption = &streams2.ReadReq_Options_NoFilter{NoFilter: &shared.Empty{}}
	}
}

func (this ReadRequest) buildFilter() *streams2.ReadReq_Options_Filter {
	filter := this.Filter.(ReadRequestFilter)
	result := &streams2.ReadReq_Options_Filter{
		Filter: &streams2.ReadReq_Options_FilterOptions{
			CheckpointIntervalMultiplier: filter.CheckpointIntervalMultiplier,
		},
	}

	switch filter.FilterBy.(type) {
	case ReadRequestFilterByEventType:
		eventTypeFilter := filter.FilterBy.(ReadRequestFilterByEventType)

		result.Filter.Filter = &streams2.ReadReq_Options_FilterOptions_EventType{
			EventType: &streams2.ReadReq_Options_FilterOptions_Expression{
				Regex:  eventTypeFilter.Regex,
				Prefix: eventTypeFilter.Prefix,
			},
		}

	case ReadRequestFilterByStreamIdentifier:
		streamIdentifierFilter := filter.FilterBy.(ReadRequestFilterByStreamIdentifier)

		result.Filter.Filter = &streams2.ReadReq_Options_FilterOptions_StreamIdentifier{
			StreamIdentifier: &streams2.ReadReq_Options_FilterOptions_Expression{
				Regex:  streamIdentifierFilter.Regex,
				Prefix: streamIdentifierFilter.Prefix,
			},
		}
	}

	switch filter.Window.(type) {
	case ReadRequestFilterWindowMax:
		maxWindow := filter.Window.(ReadRequestFilterWindowMax)

		result.Filter.Window = &streams2.ReadReq_Options_FilterOptions_Max{
			Max: maxWindow.Max,
		}

	case ReadRequestFilterWindowCount:
		result.Filter.Window = &streams2.ReadReq_Options_FilterOptions_Count{
			Count: &shared.Empty{},
		}
	}

	return result
}

type isReadRequestFilterOption interface {
	isReadRequestFilterOption()
}

type ReadRequestFilter struct {
	// ReadRequestFilterByEventType
	// ReadRequestFilterByStreamIdentifier
	FilterBy isReadRequestFilterBy
	// ReadRequestFilterWindowMax
	// ReadRequestFilterWindowCount
	Window                       isReadRequestFilterWindow
	CheckpointIntervalMultiplier uint32
}

func (this ReadRequestFilter) isReadRequestFilterOption() {}

type isReadRequestFilterBy interface {
	isReadRequestFilterBy()
}

type ReadRequestFilterByEventType struct {
	Regex  string
	Prefix []string
}

func (this ReadRequestFilterByEventType) isReadRequestFilterBy() {}

type ReadRequestFilterByStreamIdentifier struct {
	Regex  string
	Prefix []string
}

func (this ReadRequestFilterByStreamIdentifier) isReadRequestFilterBy() {}

type isReadRequestFilterWindow interface {
	isReadRequestFilterWindow()
}

type ReadRequestFilterWindowMax struct {
	Max uint32
}

func (this ReadRequestFilterWindowMax) isReadRequestFilterWindow() {}

type ReadRequestFilterWindowCount struct{}

func (this ReadRequestFilterWindowCount) isReadRequestFilterWindow() {}

type ReadRequestNoFilter struct{}

func (this ReadRequestNoFilter) isReadRequestFilterOption() {}

type isReadRequestStreamOptions interface {
	isReadRequestStreamOptions()
}

type ReadRequestStreamOptions struct {
	StreamIdentifier string
	// ReadRequestOptionsStreamRevision
	// ReadRequestOptionsStreamRevisionStart
	// ReadRequestOptionsStreamRevisionEnd
	Revision IsReadRequestStreamOptionsStreamRevision
}

func (this ReadRequestStreamOptions) isReadRequestStreamOptions() {}

type IsReadRequestStreamOptionsStreamRevision interface {
	isReadRequestStreamOptionsStreamRevision()
}

type ReadRequestOptionsStreamRevision struct {
	Revision uint64
}

func (this ReadRequestOptionsStreamRevision) isReadRequestStreamOptionsStreamRevision() {}

type ReadRequestOptionsStreamRevisionStart struct{}

func (this ReadRequestOptionsStreamRevisionStart) isReadRequestStreamOptionsStreamRevision() {}

type ReadRequestOptionsStreamRevisionEnd struct{}

func (this ReadRequestOptionsStreamRevisionEnd) isReadRequestStreamOptionsStreamRevision() {}

type ReadRequestStreamOptionsAll struct {
	// ReadRequestOptionsAllPosition
	// ReadRequestOptionsAllStartPosition
	// ReadRequestOptionsAllEndPosition
	Position IsReadRequestOptionsAllPosition
}

func (this ReadRequestStreamOptionsAll) isReadRequestStreamOptions() {}

type IsReadRequestOptionsAllPosition interface {
	isReadRequestOptionsAllPosition()
}

type ReadRequestOptionsAllPosition struct {
	CommitPosition  uint64
	PreparePosition uint64
}

func (this ReadRequestOptionsAllPosition) isReadRequestOptionsAllPosition() {}

type ReadRequestOptionsAllStartPosition struct{}

func (this ReadRequestOptionsAllStartPosition) isReadRequestOptionsAllPosition() {}

type ReadRequestOptionsAllEndPosition struct{}

func (this ReadRequestOptionsAllEndPosition) isReadRequestOptionsAllPosition() {}

type ReadRequestDirection string

const (
	ReadRequestDirectionForward  ReadRequestDirection = "ReadRequestDirectionForward"
	ReadRequestDirectionBackward ReadRequestDirection = "ReadRequestDirectionBackward"
)
