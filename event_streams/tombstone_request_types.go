package event_streams

import (
	"github.com/pivonroll/EventStore-Client-Go/protos/shared"
	"github.com/pivonroll/EventStore-Client-Go/protos/streams2"
)

type TombstoneRequest struct {
	StreamIdentifier string
	// Types that are assignable to ExpectedStreamRevision:
	// WriteStreamRevision
	// WriteStreamRevisionNoStream
	// WriteStreamRevisionAny
	// WriteStreamRevisionStreamExists
	ExpectedStreamRevision IsWriteStreamRevision
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
	case WriteStreamRevision:
		revision := this.ExpectedStreamRevision.(WriteStreamRevision)
		result.Options.ExpectedStreamRevision = &streams2.TombstoneReq_Options_Revision{
			Revision: revision.Revision,
		}
	case WriteStreamRevisionNoStream:
		result.Options.ExpectedStreamRevision = &streams2.TombstoneReq_Options_NoStream{
			NoStream: &shared.Empty{},
		}
	case WriteStreamRevisionAny:
		result.Options.ExpectedStreamRevision = &streams2.TombstoneReq_Options_Any{
			Any: &shared.Empty{},
		}
	case WriteStreamRevisionStreamExists:
		result.Options.ExpectedStreamRevision = &streams2.TombstoneReq_Options_StreamExists{
			StreamExists: &shared.Empty{},
		}
	}

	return result
}
