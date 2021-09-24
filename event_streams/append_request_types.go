package event_streams

import (
	"github.com/gofrs/uuid"
	"github.com/pivonroll/EventStore-Client-Go/protos/shared"
	"github.com/pivonroll/EventStore-Client-Go/protos/streams2"
)

type AppendRequest struct {
	// AppendRequestContentOptions
	// AppendRequestContentProposedMessage
	Content isAppendRequestContent
}

func (this *AppendRequest) Build() *streams2.AppendReq {
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

func (this *AppendRequest) buildExpectedStreamRevision(
	options *streams2.AppendReq_Options_,
	content AppendRequestContentOptions) {
	switch content.ExpectedStreamRevision.(type) {
	case AppendRequestExpectedStreamRevision:
		revision := content.ExpectedStreamRevision.(AppendRequestExpectedStreamRevision)

		options.Options.ExpectedStreamRevision = &streams2.AppendReq_Options_Revision{
			Revision: revision.Revision,
		}
	case AppendRequestExpectedStreamRevisionNoStream:
		options.Options.ExpectedStreamRevision = &streams2.AppendReq_Options_NoStream{
			NoStream: &shared.Empty{},
		}

	case AppendRequestExpectedStreamRevisionAny:
		options.Options.ExpectedStreamRevision = &streams2.AppendReq_Options_Any{
			Any: &shared.Empty{},
		}

	case AppendRequestExpectedStreamRevisionStreamExists:
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
	// AppendRequestExpectedStreamRevision
	// AppendRequestExpectedStreamRevisionNoStream
	// AppendRequestExpectedStreamRevisionAny
	// AppendRequestExpectedStreamRevisionStreamExists
	ExpectedStreamRevision IsAppendRequestExpectedStreamRevision
}

func (this AppendRequestContentOptions) isAppendRequestContent() {}

type IsAppendRequestExpectedStreamRevision interface {
	isAppendRequestExpectedStreamRevision()
}

type AppendRequestExpectedStreamRevision struct {
	Revision uint64
}

func (this AppendRequestExpectedStreamRevision) isAppendRequestExpectedStreamRevision() {}

type AppendRequestExpectedStreamRevisionNoStream struct{}

func (this AppendRequestExpectedStreamRevisionNoStream) isAppendRequestExpectedStreamRevision() {}

type AppendRequestExpectedStreamRevisionAny struct{}

func (this AppendRequestExpectedStreamRevisionAny) isAppendRequestExpectedStreamRevision() {}

type AppendRequestExpectedStreamRevisionStreamExists struct{}

func (this AppendRequestExpectedStreamRevisionStreamExists) isAppendRequestExpectedStreamRevision() {}

type AppendRequestContentProposedMessage struct {
	Id             uuid.UUID
	Metadata       map[string]string
	CustomMetadata []byte
	Data           []byte
}

func (this AppendRequestContentProposedMessage) isAppendRequestContent() {}
