package event_streams

import (
	"time"

	"github.com/google/uuid"
	"github.com/pivonroll/EventStore-Client-Go/protos/shared"
	"github.com/pivonroll/EventStore-Client-Go/protos/streams2"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type BatchAppendRequest struct {
	CorrelationId    uuid.UUID
	Options          BatchAppendRequestOptions
	ProposedMessages []BatchAppendRequestProposedMessage
	IsFinal          bool
}

func (this BatchAppendRequest) Build() *streams2.BatchAppendReq {
	result := &streams2.BatchAppendReq{
		CorrelationId: &shared.UUID{
			Value: &shared.UUID_String_{
				String_: this.CorrelationId.String(),
			},
		},
		Options: &streams2.BatchAppendReq_Options{
			StreamIdentifier: &shared.StreamIdentifier{
				StreamName: []byte(this.Options.StreamIdentifier),
			},
			Deadline: timestamppb.New(this.Options.Deadline),
		},
		ProposedMessages: this.buildProposedMessages(),
		IsFinal:          this.IsFinal,
	}

	this.buildExpectedStreamPosition(
		result,
		this.Options.ExpectedStreamRevision)

	return result
}

func (this BatchAppendRequest) buildExpectedStreamPosition(
	protoResult *streams2.BatchAppendReq,
	position IsWriteStreamRevision) {

	switch position.(type) {
	case WriteStreamRevision:
		streamPosition := position.(WriteStreamRevision)
		protoResult.Options.ExpectedStreamPosition = &streams2.BatchAppendReq_Options_StreamPosition{
			StreamPosition: streamPosition.Revision,
		}
	case WriteStreamRevisionNoStream:
		protoResult.Options.ExpectedStreamPosition = &streams2.BatchAppendReq_Options_NoStream{
			NoStream: &emptypb.Empty{},
		}

	case WriteStreamRevisionAny:
		protoResult.Options.ExpectedStreamPosition = &streams2.BatchAppendReq_Options_Any{
			Any: &emptypb.Empty{},
		}

	case WriteStreamRevisionStreamExists:
		protoResult.Options.ExpectedStreamPosition = &streams2.BatchAppendReq_Options_StreamExists{
			StreamExists: &emptypb.Empty{},
		}
	}
}

func (this BatchAppendRequest) buildProposedMessages() []*streams2.BatchAppendReq_ProposedMessage {
	result := make([]*streams2.BatchAppendReq_ProposedMessage, len(this.ProposedMessages))

	for index, value := range this.ProposedMessages {
		result[index] = &streams2.BatchAppendReq_ProposedMessage{
			Id: &shared.UUID{
				Value: &shared.UUID_String_{
					String_: value.Id.String(),
				},
			},
			Metadata:       value.Metadata,
			CustomMetadata: value.CustomMetadata,
			Data:           value.Data,
		}
	}

	return result
}

type BatchAppendRequestProposedMessage struct {
	Id             uuid.UUID
	Metadata       map[string]string
	CustomMetadata []byte
	Data           []byte
}

type BatchAppendRequestOptions struct {
	StreamIdentifier string
	// WriteStreamRevision
	// WriteStreamRevisionNoStream
	// WriteStreamRevisionAny
	// WriteStreamRevisionStreamExists
	ExpectedStreamRevision IsWriteStreamRevision
	Deadline               time.Time
}
