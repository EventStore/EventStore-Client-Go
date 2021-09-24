package event_streams

import (
	"github.com/pivonroll/EventStore-Client-Go/protos/shared"
	"github.com/pivonroll/EventStore-Client-Go/protos/streams2"
)

type TombstoneRequest struct {
	StreamIdentifier string
	// Types that are assignable to ExpectedStreamRevision:
	//	TombstoneRequestExpectedStreamRevision
	//	TombstoneRequestExpectedStreamRevisionNoStream
	//	TombstoneRequestExpectedStreamRevisionAny
	//	TombstoneRequestExpectedStreamRevisionStreamExists
	ExpectedStreamRevision IsTombstoneRequestExpectedStreamRevision
}

func (this TombstoneRequest) Build() *streams2.TombstoneReq {
	result := &streams2.TombstoneReq{
		Options: &streams2.TombstoneReq_Options{
			StreamIdentifier: &shared.StreamIdentifier{
				StreamName: []byte(this.StreamIdentifier),
			},
			ExpectedStreamRevision: nil,
		},
	}

	switch this.ExpectedStreamRevision.(type) {
	case TombstoneRequestExpectedStreamRevision:
		revision := this.ExpectedStreamRevision.(TombstoneRequestExpectedStreamRevision)
		result.Options.ExpectedStreamRevision = &streams2.TombstoneReq_Options_Revision{
			Revision: revision.Revision,
		}
	case TombstoneRequestExpectedStreamRevisionNoStream:
		result.Options.ExpectedStreamRevision = &streams2.TombstoneReq_Options_NoStream{
			NoStream: &shared.Empty{},
		}
	case TombstoneRequestExpectedStreamRevisionAny:
		result.Options.ExpectedStreamRevision = &streams2.TombstoneReq_Options_Any{
			Any: &shared.Empty{},
		}
	case TombstoneRequestExpectedStreamRevisionStreamExists:
		result.Options.ExpectedStreamRevision = &streams2.TombstoneReq_Options_StreamExists{
			StreamExists: &shared.Empty{},
		}
	}

	return result
}

type IsTombstoneRequestExpectedStreamRevision interface {
	isTombstoneRequestExpectedStreamRevision()
}

type TombstoneRequestExpectedStreamRevision struct {
	Revision uint64
}

func (this TombstoneRequestExpectedStreamRevision) isTombstoneRequestExpectedStreamRevision() {
}

type TombstoneRequestExpectedStreamRevisionNoStream struct{}

func (this TombstoneRequestExpectedStreamRevisionNoStream) isTombstoneRequestExpectedStreamRevision() {
}

type TombstoneRequestExpectedStreamRevisionAny struct{}

func (this TombstoneRequestExpectedStreamRevisionAny) isTombstoneRequestExpectedStreamRevision() {
}

type TombstoneRequestExpectedStreamRevisionStreamExists struct{}

func (this TombstoneRequestExpectedStreamRevisionStreamExists) isTombstoneRequestExpectedStreamRevision() {
}
