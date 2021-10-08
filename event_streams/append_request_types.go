package event_streams

import (
	"github.com/google/uuid"
	"github.com/pivonroll/EventStore-Client-Go/protos/shared"
	"github.com/pivonroll/EventStore-Client-Go/protos/streams2"
)

type appendRequest struct {
	// AppendRequestContentOptions
	// AppendRequestContentProposedMessage
	Content isAppendRequestContent
}

func (this *appendRequest) Build() *streams2.AppendReq {
	result := &streams2.AppendReq{
		Content: nil,
	}

	switch this.Content.(type) {
	case AppendRequestContentOptions:
		content := this.Content.(AppendRequestContentOptions)

		result.Content = &streams2.AppendReq_Options_{
			Options: &streams2.AppendReq_Options{
				StreamIdentifier: &shared.StreamIdentifier{
					StreamName: []byte(content.StreamIdentifier),
				},
				ExpectedStreamRevision: nil,
			},
		}

		this.buildExpectedStreamRevision(result.Content.(*streams2.AppendReq_Options_), content)

	case AppendRequestContentProposedMessage:
		content := this.Content.(AppendRequestContentProposedMessage)

		result.Content = &streams2.AppendReq_ProposedMessage_{
			ProposedMessage: &streams2.AppendReq_ProposedMessage{
				Id: &shared.UUID{
					Value: &shared.UUID_String_{
						String_: content.Id.String(),
					},
				},
				Metadata:       content.Metadata,
				CustomMetadata: content.CustomMetadata,
				Data:           content.Data,
			},
		}
	}

	return result
}

func (this *appendRequest) buildExpectedStreamRevision(
	options *streams2.AppendReq_Options_,
	content AppendRequestContentOptions) {
	switch content.ExpectedStreamRevision.(type) {
	case WriteStreamRevision:
		revision := content.ExpectedStreamRevision.(WriteStreamRevision)

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

type AppendRequestContentOptions struct {
	StreamIdentifier string
	// WriteStreamRevision
	// WriteStreamRevisionNoStream
	// WriteStreamRevisionAny
	// WriteStreamRevisionStreamExists
	ExpectedStreamRevision IsWriteStreamRevision
}

func (this AppendRequestContentOptions) isAppendRequestContent() {}

type AppendRequestContentProposedMessage struct {
	Id             uuid.UUID
	Metadata       map[string]string
	CustomMetadata []byte
	Data           []byte
}

func (this AppendRequestContentProposedMessage) isAppendRequestContent() {}
