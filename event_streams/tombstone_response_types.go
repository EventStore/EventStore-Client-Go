package event_streams

import "github.com/pivonroll/EventStore-Client-Go/protos/streams2"

// TombstoneResponse is response received when stream is hard-deleted by using Client.TombstoneStream.
type TombstoneResponse struct {
	position isTombstoneResponsePosition
}

// GetPosition returns a position at which stream was hard-deleted.
// If position was received it will also return a true as a second return value.
// If position does not exist a zero initialized Position and a false will be returned.
// Position may not exist if an empty stream was hard-deleted.
func (response TombstoneResponse) GetPosition() (Position, bool) {
	if response.position != nil {
		if position, isPosition := response.position.(tombstoneResponsePosition); isPosition {
			return Position{
				CommitPosition:  position.CommitPosition,
				PreparePosition: position.PreparePosition,
			}, true
		}
	}

	return Position{}, false
}

type isTombstoneResponsePosition interface {
	isTombstoneResponsePosition()
}

type tombstoneResponsePosition struct {
	CommitPosition  uint64
	PreparePosition uint64
}

func (this tombstoneResponsePosition) isTombstoneResponsePosition() {
}

type tombstoneResponseAdapter interface {
	Create(protoTombstone *streams2.TombstoneResp) TombstoneResponse
}

type tombstoneResponseAdapterImpl struct{}

func (this tombstoneResponseAdapterImpl) Create(protoTombstone *streams2.TombstoneResp) TombstoneResponse {
	result := TombstoneResponse{}

	switch protoTombstone.PositionOption.(type) {
	case *streams2.TombstoneResp_Position_:
		protoPosition := protoTombstone.PositionOption.(*streams2.TombstoneResp_Position_)
		result.position = tombstoneResponsePosition{
			CommitPosition:  protoPosition.Position.CommitPosition,
			PreparePosition: protoPosition.Position.PreparePosition,
		}
	case *streams2.TombstoneResp_NoPosition:
		result.position = nil
	}

	return result
}
