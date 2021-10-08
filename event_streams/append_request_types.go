package event_streams

import (
	"github.com/google/uuid"
	"github.com/pivonroll/EventStore-Client-Go/protos/shared"
	"github.com/pivonroll/EventStore-Client-Go/protos/streams2"
)

type appendRequest struct {
	content isAppendRequestContent
}

func (this *appendRequest) build() *streams2.AppendReq {
	result := &streams2.AppendReq{
		Content: nil,
	}

	switch this.content.(type) {
	case appendRequestContentOptions:
		content := this.content.(appendRequestContentOptions)

		result.Content = &streams2.AppendReq_Options_{
			Options: &streams2.AppendReq_Options{
				StreamIdentifier: &shared.StreamIdentifier{
					StreamName: []byte(content.streamId),
				},
				ExpectedStreamRevision: nil,
			},
		}

		this.buildExpectedStreamRevision(result.Content.(*streams2.AppendReq_Options_), content)

	case appendRequestContentProposedMessage:
		content := this.content.(appendRequestContentProposedMessage)

		result.Content = &streams2.AppendReq_ProposedMessage_{
			ProposedMessage: &streams2.AppendReq_ProposedMessage{
				Id: &shared.UUID{
					Value: &shared.UUID_String_{
						String_: content.eventId.String(),
					},
				},
				Metadata:       content.metadata,
				CustomMetadata: content.customMetadata,
				Data:           content.data,
			},
		}
	}

	return result
}

func (this *appendRequest) buildExpectedStreamRevision(
	options *streams2.AppendReq_Options_,
	content appendRequestContentOptions) {
	switch content.expectedStreamRevision.(type) {
	case WriteStreamRevision:
		revision := content.expectedStreamRevision.(WriteStreamRevision)

		options.Options.ExpectedStreamRevision = &streams2.AppendReq_Options_Revision{
			Revision: revision.Revision,
		}
	case WriteStreamRevisionNoStream:
		options.Options.ExpectedStreamRevision = &streams2.AppendReq_Options_NoStream{
			NoStream: &shared.Empty{},
		}

	case WriteStreamRevisionAny:
		options.Options.ExpectedStreamRevision = &streams2.AppendReq_Options_Any{
			Any: &shared.Empty{},
		}

	case WriteStreamRevisionStreamExists:
		options.Options.ExpectedStreamRevision = &streams2.AppendReq_Options_StreamExists{
			StreamExists: &shared.Empty{},
		}
	}
}

type isAppendRequestContent interface {
	isAppendRequestContent()
}

type appendRequestContentOptions struct {
	streamId string
	// WriteStreamRevision
	// WriteStreamRevisionNoStream
	// WriteStreamRevisionAny
	// WriteStreamRevisionStreamExists
	expectedStreamRevision IsWriteStreamRevision
}

func (this appendRequestContentOptions) isAppendRequestContent() {}

type appendRequestContentProposedMessage struct {
	eventId        uuid.UUID
	metadata       map[string]string
	customMetadata []byte
	data           []byte
}

func (this appendRequestContentProposedMessage) isAppendRequestContent() {}
