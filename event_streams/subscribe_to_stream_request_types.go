package event_streams

import (
	"github.com/pivonroll/EventStore-Client-Go/protos/shared"
	"github.com/pivonroll/EventStore-Client-Go/protos/streams2"
)

type subscribeToStreamRequest struct {
	streamOption isSubscribeRequestStreamOptions
	direction    subscribeRequestDirection
	resolveLinks bool
	// Filter
	// noFilter
	filter isFilter
}

func (this subscribeToStreamRequest) build() *streams2.ReadReq {
	result := &streams2.ReadReq{
		Options: &streams2.ReadReq_Options{
			ResolveLinks: this.resolveLinks,
			FilterOption: nil,
			CountOption: &streams2.ReadReq_Options_Subscription{
				Subscription: &streams2.ReadReq_Options_SubscriptionOptions{},
			},
			UuidOption: &streams2.ReadReq_Options_UUIDOption{
				Content: &streams2.ReadReq_Options_UUIDOption_String_{
					String_: &shared.Empty{},
				},
			},
		},
	}

	if this.direction == subscribeRequestDirectionForward {
		result.Options.ReadDirection = streams2.ReadReq_Options_Forwards
	} else {
		result.Options.ReadDirection = streams2.ReadReq_Options_Backwards
	}

	this.buildStreamOption(result.Options)
	this.buildFilterOption(result.Options)

	return result
}

func (this subscribeToStreamRequest) buildStreamOption(options *streams2.ReadReq_Options) {
	switch this.streamOption.(type) {
	case subscribeRequestStreamOptions:
		options.StreamOption = this.buildStreamOptions(
			this.streamOption.(subscribeRequestStreamOptions))
	case subscribeRequestStreamOptionsAll:
		options.StreamOption = this.buildStreamOptionAll(
			this.streamOption.(subscribeRequestStreamOptionsAll))
	}
}

func (this subscribeToStreamRequest) buildStreamOptionAll(all subscribeRequestStreamOptionsAll) *streams2.ReadReq_Options_All {
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

func (this subscribeToStreamRequest) buildStreamOptions(
	streamOptions subscribeRequestStreamOptions) *streams2.ReadReq_Options_Stream {
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

func (this subscribeToStreamRequest) buildFilterOption(options *streams2.ReadReq_Options) {
	switch this.filter.(type) {
	case Filter:
		options.FilterOption = buildProtoFilter(this.filter)

	case noFilter:
		options.FilterOption = &streams2.ReadReq_Options_NoFilter{NoFilter: &shared.Empty{}}
	}
}

type isSubscribeRequestStreamOptions interface {
	isSubscribeRequestStreamOptions()
}

type subscribeRequestStreamOptions struct {
	StreamIdentifier string
	// ReadStreamRevision
	// ReadStreamRevisionStart
	// ReadStreamRevisionEnd
	Revision IsReadStreamRevision
}

func (this subscribeRequestStreamOptions) isSubscribeRequestStreamOptions() {}

type subscribeRequestStreamOptionsAll struct {
	// ReadPositionAll
	// ReadPositionAllStart
	// ReadPositionAllEnd
	Position IsReadPositionAll
}

func (this subscribeRequestStreamOptionsAll) isSubscribeRequestStreamOptions() {}

type subscribeRequestDirection string

const (
	subscribeRequestDirectionForward subscribeRequestDirection = "subscribeRequestDirectionForward"
	subscribeRequestDirectionEnd     subscribeRequestDirection = "subscribeRequestDirectionEnd"
)
