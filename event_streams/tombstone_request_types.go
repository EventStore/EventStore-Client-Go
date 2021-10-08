package event_streams

import (
	"github.com/pivonroll/EventStore-Client-Go/protos/shared"
	"github.com/pivonroll/EventStore-Client-Go/protos/streams2"
)

type tombstoneRequest struct {
	streamId string
	// Types that are assignable to expectedStreamRevision:
	// WriteStreamRevision
	// WriteStreamRevisionNoStream
	// WriteStreamRevisionAny
	// WriteStreamRevisionStreamExists
	expectedStreamRevision IsWriteStreamRevision
}

func (this tombstoneRequest) build() *streams2.TombstoneReq {
	result := &streams2.TombstoneReq{
		Options: &streams2.TombstoneReq_Options{
			StreamIdentifier: &shared.StreamIdentifier{
				StreamName: []byte(this.streamId),
			},
			ExpectedStreamRevision: nil,
		},
	}

	switch this.expectedStreamRevision.(type) {
	case WriteStreamRevision:
		revision := this.expectedStreamRevision.(WriteStreamRevision)
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
