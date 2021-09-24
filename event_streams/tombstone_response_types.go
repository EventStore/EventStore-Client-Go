package event_streams

import "github.com/pivonroll/EventStore-Client-Go/protos/streams2"

type TombstoneResponse struct {
	// Types that are assignable to PositionOption:
	//	TombstoneResponsePosition
	//	TombstoneResponseNoPosition
	Position isTombstoneResponsePosition
}

func (response TombstoneResponse) GetCommitPosition() (uint64, bool) {
	if position, isPosition := response.Position.(TombstoneResponsePosition); isPosition {
		return position.CommitPosition, true
	}

	return 0, false
}

func (response TombstoneResponse) GetPreparePosition() (uint64, bool) {
	if position, isPosition := response.Position.(TombstoneResponsePosition); isPosition {
		return position.PreparePosition, true
	}

	return 0, false
}

func (response TombstoneResponse) GetPosition() (Position, bool) {
	if position, isPosition := response.Position.(TombstoneResponsePosition); isPosition {
		return Position{
			CommitPosition:  position.CommitPosition,
			PreparePosition: position.PreparePosition,
		}, true
	}

	return Position{}, false
}

type isTombstoneResponsePosition interface {
	isTombstoneResponsePosition()
}

type TombstoneResponsePosition struct {
	CommitPosition  uint64
	PreparePosition uint64
}

func (this TombstoneResponsePosition) isTombstoneResponsePosition() {
}

type TombstoneResponseNoPosition struct{}

func (this TombstoneResponseNoPosition) isTombstoneResponsePosition() {
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
		result.Position = TombstoneResponsePosition{
			CommitPosition:  protoPosition.Position.CommitPosition,
			PreparePosition: protoPosition.Position.PreparePosition,
		}
	case *streams2.TombstoneResp_NoPosition:
		result.Position = TombstoneResponseNoPosition{}
	}

	return result
}
