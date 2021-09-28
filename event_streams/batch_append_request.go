package event_streams

import (
	"time"

	"github.com/gofrs/uuid"
	"github.com/pivonroll/EventStore-Client-Go/protos/shared"
	"github.com/pivonroll/EventStore-Client-Go/protos/streams2"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type BatchAppendRequest struct {
	CorrelationId    string
	Options          BatchAppendRequestOptions
	ProposedMessages []BatchAppendRequestProposedMessage
	IsFinal          bool
}

func (this BatchAppendRequest) Build() *streams2.BatchAppendReq {
	result := &streams2.BatchAppendReq{
		CorrelationId: &shared.UUID{
			Value: &shared.UUID_String_{
				String_: this.CorrelationId,
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
		this.Options.ExpectedStreamPosition)

	return result
}

func (this BatchAppendRequest) buildExpectedStreamPosition(
	protoResult *streams2.BatchAppendReq,
	position isBatchAppendExpectedStreamPosition) {

	switch position.(type) {
	case BatchAppendExpectedStreamPosition:
		streamPosition := position.(BatchAppendExpectedStreamPosition)
		protoResult.Options.ExpectedStreamPosition = &streams2.BatchAppendReq_Options_StreamPosition{
			StreamPosition: streamPosition.StreamPosition,
		}
	case BatchAppendExpectedStreamPositionNoStream:
		protoResult.Options.ExpectedStreamPosition = &streams2.BatchAppendReq_Options_NoStream{
			NoStream: &emptypb.Empty{},
		}

	case BatchAppendExpectedStreamPositionAny:
		protoResult.Options.ExpectedStreamPosition = &streams2.BatchAppendReq_Options_Any{
			Any: &emptypb.Empty{},
		}

	case BatchAppendExpectedStreamPositionStreamExists:
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
	//	BatchAppendExpectedStreamPosition
	//	BatchAppendExpectedStreamPositionNoStream
	//	BatchAppendExpectedStreamPositionAny
	//	BatchAppendExpectedStreamPositionStreamExists
	ExpectedStreamPosition isBatchAppendExpectedStreamPosition
	Deadline               time.Time
}

type isBatchAppendExpectedStreamPosition interface {
	isBatchAppendExpectedStreamPosition()
}

type BatchAppendExpectedStreamPosition struct {
	StreamPosition uint64
}

func (this BatchAppendExpectedStreamPosition) isBatchAppendExpectedStreamPosition() {
}

type BatchAppendExpectedStreamPositionNoStream struct{}

func (this BatchAppendExpectedStreamPositionNoStream) isBatchAppendExpectedStreamPosition() {
}

type BatchAppendExpectedStreamPositionAny struct{}

func (this BatchAppendExpectedStreamPositionAny) isBatchAppendExpectedStreamPosition() {
}

type BatchAppendExpectedStreamPositionStreamExists struct{}

func (this BatchAppendExpectedStreamPositionStreamExists) isBatchAppendExpectedStreamPosition() {
}
