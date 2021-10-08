package event_streams

import (
	"time"

	"github.com/google/uuid"
	"github.com/pivonroll/EventStore-Client-Go/protos/shared"
	"github.com/pivonroll/EventStore-Client-Go/protos/streams2"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type batchAppendRequest struct {
	correlationId    uuid.UUID
	options          batchAppendRequestOptions
	proposedMessages []batchAppendRequestProposedMessage
	isFinal          bool
}

func (this batchAppendRequest) build() *streams2.BatchAppendReq {
	result := &streams2.BatchAppendReq{
		CorrelationId: &shared.UUID{
			Value: &shared.UUID_String_{
				String_: this.correlationId.String(),
			},
		},
		Options: &streams2.BatchAppendReq_Options{
			StreamIdentifier: &shared.StreamIdentifier{
				StreamName: []byte(this.options.streamId),
			},
			Deadline: timestamppb.New(this.options.deadline),
		},
		ProposedMessages: this.buildProposedMessages(),
		IsFinal:          this.isFinal,
	}

	this.buildExpectedStreamPosition(
		result,
		this.options.expectedStreamRevision)

	return result
}

func (this batchAppendRequest) buildExpectedStreamPosition(
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

func (this batchAppendRequest) buildProposedMessages() []*streams2.BatchAppendReq_ProposedMessage {
	result := make([]*streams2.BatchAppendReq_ProposedMessage, len(this.proposedMessages))

	for index, value := range this.proposedMessages {
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

type batchAppendRequestProposedMessage struct {
	Id             uuid.UUID
	Metadata       map[string]string
	CustomMetadata []byte
	Data           []byte
}

type batchAppendRequestOptions struct {
	streamId string
	// WriteStreamRevision
	// WriteStreamRevisionNoStream
	// WriteStreamRevisionAny
	// WriteStreamRevisionStreamExists
	expectedStreamRevision IsWriteStreamRevision
	deadline               time.Time
}
