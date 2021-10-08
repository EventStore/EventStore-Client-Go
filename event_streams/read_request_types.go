package event_streams

import (
	"github.com/pivonroll/EventStore-Client-Go/protos/shared"
	"github.com/pivonroll/EventStore-Client-Go/protos/streams2"
)

const ReadCountMax = ^uint64(0)

type readRequest struct {
	streamOption isReadRequestStreamOptions
	direction    ReadDirection
	resolveLinks bool
	count        uint64
	// Filter
	// noFilter
	filter isFilter
}

func (this readRequest) build() *streams2.ReadReq {
	result := &streams2.ReadReq{
		Options: &streams2.ReadReq_Options{
			ResolveLinks: this.resolveLinks,
			FilterOption: nil,
			UuidOption: &streams2.ReadReq_Options_UUIDOption{
				Content: &streams2.ReadReq_Options_UUIDOption_String_{
					String_: &shared.Empty{},
				},
			},
			CountOption: &streams2.ReadReq_Options_Count{
				Count: this.count,
			},
		},
	}

	if this.direction == ReadDirectionForward {
		result.Options.ReadDirection = streams2.ReadReq_Options_Forwards
	} else {
		result.Options.ReadDirection = streams2.ReadReq_Options_Backwards
	}

	this.buildStreamOption(result.Options)
	this.buildFilterOption(result.Options)

	return result
}

func (this readRequest) buildStreamOption(options *streams2.ReadReq_Options) {
	switch this.streamOption.(type) {
	case readRequestStreamOptions:
		options.StreamOption = this.buildStreamOptions(
			this.streamOption.(readRequestStreamOptions))
	case readRequestStreamOptionsAll:
		options.StreamOption = thisBuildStreamOptionAll(
			this.streamOption.(readRequestStreamOptionsAll))
	}
}

func thisBuildStreamOptionAll(all readRequestStreamOptionsAll) *streams2.ReadReq_Options_All {
	result := &streams2.ReadReq_Options_All{
		All: &streams2.ReadReq_Options_AllOptions{},
	}

	switch all.Position.(type) {
	case ReadPositionAll:
		allPosition := all.Position.(ReadPositionAll)
		result.All.AllOption = &streams2.ReadReq_Options_AllOptions_Position{
			Position: &streams2.ReadReq_Options_Position{
				CommitPosition:  allPosition.CommitPosition,
				PreparePosition: allPosition.PreparePosition,
			},
		}
	case ReadPositionAllStart:
		result.All.AllOption = &streams2.ReadReq_Options_AllOptions_Start{
			Start: &shared.Empty{},
		}
	case ReadPositionAllEnd:
		result.All.AllOption = &streams2.ReadReq_Options_AllOptions_End{
			End: &shared.Empty{},
		}
	}

	return result
}

func (this readRequest) buildStreamOptions(
	streamOptions readRequestStreamOptions) *streams2.ReadReq_Options_Stream {
	result := &streams2.ReadReq_Options_Stream{
		Stream: &streams2.ReadReq_Options_StreamOptions{
			StreamIdentifier: &shared.StreamIdentifier{
				StreamName: []byte(streamOptions.StreamIdentifier),
			},
			RevisionOption: nil,
		},
	}

	switch streamOptions.Revision.(type) {
	case ReadStreamRevision:
		streamRevision := streamOptions.Revision.(ReadStreamRevision)
		result.Stream.RevisionOption = &streams2.ReadReq_Options_StreamOptions_Revision{
			Revision: streamRevision.Revision,
		}
	case ReadStreamRevisionStart:
		result.Stream.RevisionOption = &streams2.ReadReq_Options_StreamOptions_Start{
			Start: &shared.Empty{},
		}
	case ReadStreamRevisionEnd:
		result.Stream.RevisionOption = &streams2.ReadReq_Options_StreamOptions_End{
			End: &shared.Empty{},
		}
	}

	return result
}

func (this readRequest) buildFilterOption(options *streams2.ReadReq_Options) {
	switch this.filter.(type) {
	case Filter:
		options.FilterOption = buildProtoFilter(this.filter)

	case noFilter:
		options.FilterOption = &streams2.ReadReq_Options_NoFilter{NoFilter: &shared.Empty{}}
	}
}

type isReadRequestStreamOptions interface {
	isReadRequestStreamOptions()
}

type readRequestStreamOptions struct {
	StreamIdentifier string
	// ReadStreamRevision
	// ReadStreamRevisionStart
	// ReadStreamRevisionEnd
	Revision IsReadStreamRevision
}

func (this readRequestStreamOptions) isReadRequestStreamOptions() {}

type readRequestStreamOptionsAll struct {
	// ReadPositionAll
	// ReadPositionAllStart
	// ReadPositionAllEnd
	Position IsReadPositionAll
}

func (this readRequestStreamOptionsAll) isReadRequestStreamOptions() {}
