package event_streams

import "github.com/pivonroll/EventStore-Client-Go/protos/streams2"

type DeleteResponse struct {
	// Types that are assignable to PositionOption:
	//	DeleteResponsePosition
	//	DeleteResponseNoPosition
	Position isDeleteResponsePosition
}

func (response DeleteResponse) GetPosition() (Position, bool) {
	if position, isPosition := response.Position.(DeleteResponsePosition); isPosition {
		return Position{
			CommitPosition:  position.CommitPosition,
			PreparePosition: position.PreparePosition,
		}, true
	}

	return Position{}, false
}

type isDeleteResponsePosition interface {
	isDeleteResponsePositionOption()
}

type DeleteResponsePosition struct {
	CommitPosition  uint64
	PreparePosition uint64
}

func (this DeleteResponsePosition) isDeleteResponsePositionOption() {
}

type DeleteResponseNoPosition struct{}

func (this DeleteResponseNoPosition) isDeleteResponsePositionOption() {
}

type deleteResponseAdapter interface {
	Create(resp *streams2.DeleteResp) DeleteResponse
}

type deleteResponseAdapterImpl struct{}

func (this deleteResponseAdapterImpl) Create(protoResponse *streams2.DeleteResp) DeleteResponse {
	result := DeleteResponse{}

	switch protoResponse.PositionOption.(type) {
	case *streams2.DeleteResp_Position_:
		protoPosition := protoResponse.PositionOption.(*streams2.DeleteResp_Position_)
		result.Position = DeleteResponsePosition{
			CommitPosition:  protoPosition.Position.CommitPosition,
			PreparePosition: protoPosition.Position.PreparePosition,
		}
	case *streams2.DeleteResp_NoPosition:
		result.Position = DeleteResponseNoPosition{}
	}

	return result
}
