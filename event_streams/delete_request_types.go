package event_streams

import (
	"github.com/pivonroll/EventStore-Client-Go/protos/shared"
	"github.com/pivonroll/EventStore-Client-Go/protos/streams2"
)

type DeleteRequest struct {
	StreamIdentifier string
	// Types that are assignable to ExpectedStreamRevision:
	//	DeleteRequestExpectedStreamRevision
	//	DeleteRequestExpectedStreamRevisionNoStream
	//	DeleteRequestExpectedStreamRevisionAny
	//	DeleteRequestExpectedStreamRevisionStreamExists
	ExpectedStreamRevision IsDeleteRequestExpectedStreamRevision
}

func (this DeleteRequest) Build() *streams2.DeleteReq {
	result := &streams2.DeleteReq{
		Options: &streams2.DeleteReq_Options{
			StreamIdentifier: &shared.StreamIdentifier{
				StreamName: []byte(this.StreamIdentifier),
			},
			ExpectedStreamRevision: nil,
		},
	}

	switch this.ExpectedStreamRevision.(type) {
	case DeleteRequestExpectedStreamRevision:
		revision := this.ExpectedStreamRevision.(DeleteRequestExpectedStreamRevision)
		result.Options.ExpectedStreamRevision = &streams2.DeleteReq_Options_Revision{
			Revision: revision.Revision,
		}

	case DeleteRequestExpectedStreamRevisionNoStream:
		result.Options.ExpectedStreamRevision = &streams2.DeleteReq_Options_NoStream{
			NoStream: &shared.Empty{},
		}
	case DeleteRequestExpectedStreamRevisionAny:
		result.Options.ExpectedStreamRevision = &streams2.DeleteReq_Options_Any{
			Any: &shared.Empty{},
		}
	case DeleteRequestExpectedStreamRevisionStreamExists:
		result.Options.ExpectedStreamRevision = &streams2.DeleteReq_Options_StreamExists{
			StreamExists: &shared.Empty{},
		}
	}

	return result
}

type IsDeleteRequestExpectedStreamRevision interface {
	isDeleteRequestExpectedStreamRevision()
}

type DeleteRequestExpectedStreamRevision struct {
	Revision uint64
}

func (this DeleteRequestExpectedStreamRevision) isDeleteRequestExpectedStreamRevision() {
}

type DeleteRequestExpectedStreamRevisionNoStream struct{}

func (this DeleteRequestExpectedStreamRevisionNoStream) isDeleteRequestExpectedStreamRevision() {
}

type DeleteRequestExpectedStreamRevisionAny struct{}

func (this DeleteRequestExpectedStreamRevisionAny) isDeleteRequestExpectedStreamRevision() {
}

type DeleteRequestExpectedStreamRevisionStreamExists struct{}

func (this DeleteRequestExpectedStreamRevisionStreamExists) isDeleteRequestExpectedStreamRevision() {
}
